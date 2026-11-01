//! Bridge state: sessions, in-flight requests, audit log, recent events.

use crate::output::Outbox;
use rabbitmq_amqp091::Session as Amqp091;
use rabbitmq_amqp10::Amqp10Session;
use rabbitmq_core::buffer::{AuditLog, BoundedBuffer};
use rabbitmq_core::capabilities::FeatureMatrix;
use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::events::{EventSink, SharedSink};
use rabbitmq_core::profile::Profile;
use rabbitmq_management::ManagementClient;
use rabbitmq_protocol::RequestId;
use rabbitmq_streams::StreamSession;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, OnceCell};

pub const MAX_INFLIGHT: usize = 256;

/// Sink that forwards to the outbox and keeps a bounded copy of `$/event`
/// entries for diagnostic reports.
pub struct RecordingSink {
    pub outbox: Outbox,
    pub recent: StdMutex<BoundedBuffer<Value>>,
}

impl EventSink for RecordingSink {
    fn emit(&self, method: &str, params: Value) {
        if method == "$/event" || method == "$/session" || method == "$/consumer" {
            if let Ok(mut r) = self.recent.lock() {
                r.push(json!({"method": method, "params": params.clone()}));
            }
        }
        self.outbox.notify(method, params);
    }
}

pub struct BridgeSession {
    pub id: String,
    pub profile: Profile,
    pub mgmt: Option<ManagementClient>,
    pub features: Option<FeatureMatrix>,
    amqp: OnceCell<Arc<Amqp091>>,
    stream: OnceCell<Arc<StreamSession>>,
    amqp10: OnceCell<Arc<Amqp10Session>>,
    sink: SharedSink,
    pub opened_at: u64,
}

impl BridgeSession {
    pub fn new(id: String, profile: Profile, sink: SharedSink) -> Self {
        let mgmt = profile.management_url.as_ref().and_then(|_| ManagementClient::from_profile(&profile).ok());
        Self {
            id,
            profile,
            mgmt,
            features: None,
            amqp: OnceCell::new(),
            stream: OnceCell::new(),
            amqp10: OnceCell::new(),
            sink,
            opened_at: rabbitmq_core::message::now_millis(),
        }
    }

    pub async fn amqp(&self) -> Result<Arc<Amqp091>> {
        self.amqp
            .get_or_try_init(|| Amqp091::connect(self.id.clone(), self.profile.clone(), self.sink.clone()))
            .await
            .cloned()
    }

    pub fn amqp_if_open(&self) -> Option<Arc<Amqp091>> {
        self.amqp.get().cloned()
    }

    pub async fn stream(&self) -> Result<Arc<StreamSession>> {
        self.stream
            .get_or_try_init(|| StreamSession::connect(self.id.clone(), self.profile.clone(), self.sink.clone()))
            .await
            .cloned()
    }

    pub async fn amqp10(&self) -> Result<Arc<Amqp10Session>> {
        self.amqp10
            .get_or_try_init(|| Amqp10Session::connect(self.id.clone(), self.profile.clone(), self.sink.clone()))
            .await
            .cloned()
    }

    pub fn mgmt(&self) -> Result<&ManagementClient> {
        self.mgmt.as_ref().ok_or_else(|| {
            CoreError::unsupported(format!("session {} has no Management API configured", self.id))
                .with_hint("Add :management-url to the profile to use administration and monitoring features.")
        })
    }

    pub async fn info(&self) -> Value {
        let amqp = match self.amqp.get() {
            Some(a) => serde_json::to_value(a.info().await).unwrap_or(Value::Null),
            None => Value::Null,
        };
        let stream = match self.stream.get() {
            Some(s) => json!({"connected": true, "subscriptions": s.subscriptions().await}),
            None => Value::Null,
        };
        let amqp10 = match self.amqp10.get() {
            Some(s) => json!({"connected": true, "links": s.links().await}),
            None => Value::Null,
        };
        rabbitmq_core::redact::redact_json(&json!({
            "id": self.id,
            "profile": self.profile,
            "read_only": self.profile.read_only,
            "management": self.mgmt.as_ref().map(|m| m.base_url().to_string()),
            "features": self.features,
            "opened_at": self.opened_at,
            "amqp091": amqp,
            "stream": stream,
            "amqp10": amqp10,
        }))
    }

    pub async fn close(&self) {
        if let Some(a) = self.amqp.get() {
            let _ = a.close().await;
        }
        if let Some(s) = self.stream.get() {
            let _ = s.close().await;
        }
        if let Some(s) = self.amqp10.get() {
            let _ = s.close().await;
        }
    }
}

pub struct State {
    pub outbox: Outbox,
    pub sink: Arc<RecordingSink>,
    pub sessions: Mutex<HashMap<String, Arc<BridgeSession>>>,
    pub inflight: StdMutex<HashMap<RequestId, (tokio::task::AbortHandle, &'static str)>>,
    pub audit: StdMutex<AuditLog>,
    pub started_at: u64,
}

impl State {
    pub fn new(outbox: Outbox, audit: AuditLog) -> Arc<Self> {
        let sink = Arc::new(RecordingSink { outbox: outbox.clone(), recent: StdMutex::new(BoundedBuffer::new(500)) });
        Arc::new(Self {
            outbox,
            sink,
            sessions: Mutex::new(HashMap::new()),
            inflight: StdMutex::new(HashMap::new()),
            audit: StdMutex::new(audit),
            started_at: rabbitmq_core::message::now_millis(),
        })
    }

    pub async fn session(&self, id: &str) -> Result<Arc<BridgeSession>> {
        self.sessions
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| CoreError::new(ErrorCode::NotFound, format!("no session {id}")).with_hint("Open one with session.open"))
    }

    pub fn shared_sink(&self) -> SharedSink {
        self.sink.clone()
    }

    pub async fn close_all(&self) {
        let all: Vec<_> = self.sessions.lock().await.drain().collect();
        for (_, s) in all {
            s.close().await;
        }
    }
}
