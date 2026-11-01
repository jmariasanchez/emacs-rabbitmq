//! Event emission from adapters to whoever hosts them (the bridge, a test,
//! or another embedding application).
//!
//! Sinks must be non-blocking and bounded. When a sink cannot accept an
//! event it must count the drop; adapters keep enough state (consumer
//! buffers, pending deliveries) for the frontend to recover anything a
//! dropped notification would have carried.

use serde_json::Value;
use std::sync::{Arc, Mutex};

pub trait EventSink: Send + Sync {
    /// Emit a notification. `method` is one of the `rabbitmq_protocol::notify` names.
    fn emit(&self, method: &str, params: Value);
}

pub type SharedSink = Arc<dyn EventSink>;

/// Sink that discards everything.
pub struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _method: &str, _params: Value) {}
}

/// Sink that records events in memory, for tests.
#[derive(Default)]
pub struct MemorySink {
    events: Mutex<Vec<(String, Value)>>,
}

impl MemorySink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn take(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
    pub fn snapshot(&self) -> Vec<(String, Value)> {
        self.events.lock().unwrap().clone()
    }
    pub fn count(&self, method: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .count()
    }
}

impl EventSink for MemorySink {
    fn emit(&self, method: &str, params: Value) {
        self.events
            .lock()
            .unwrap()
            .push((method.to_string(), params));
    }
}

/// Notification method names (mirrors `rabbitmq_protocol::notify`, kept here
/// so adapters do not depend on the protocol crate).
pub mod names {
    pub const SESSION: &str = "$/session";
    pub const DELIVERIES: &str = "$/deliveries";
    pub const CONSUMER: &str = "$/consumer";
    pub const RETURNED: &str = "$/returned";
    pub const EVENT: &str = "$/event";
    pub const PROGRESS: &str = "$/progress";
}

/// Convenience: emit a human-readable event log entry.
pub fn log(sink: &dyn EventSink, level: &str, source: &str, message: impl Into<String>) {
    sink.emit(
        names::EVENT,
        serde_json::json!({
            "level": level,
            "source": source,
            "message": crate::redact::redact_text(&message.into()),
            "at": crate::message::now_millis(),
        }),
    );
}
