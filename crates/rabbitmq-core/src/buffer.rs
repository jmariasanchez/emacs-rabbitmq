//! Bounded buffers and the client-side audit log.

use crate::redact::redact_json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;

/// A FIFO ring buffer with a hard capacity. When full, the oldest entry is
/// evicted and counted so the UI can report data loss explicitly.
#[derive(Debug, Clone)]
pub struct BoundedBuffer<T> {
    items: VecDeque<T>,
    capacity: usize,
    dropped: u64,
    total: u64,
}

impl<T> BoundedBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            items: VecDeque::with_capacity(capacity.min(1024)),
            capacity: capacity.max(1),
            dropped: 0,
            total: 0,
        }
    }

    /// Push an item; returns the evicted item, if any.
    pub fn push(&mut self, item: T) -> Option<T> {
        self.total += 1;
        let evicted = if self.items.len() >= self.capacity {
            self.dropped += 1;
            self.items.pop_front()
        } else {
            None
        };
        self.items.push_back(item);
        evicted
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn drain(&mut self, max: usize) -> Vec<T> {
        let n = max.min(self.items.len());
        self.items.drain(..n).collect()
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
    pub fn clear(&mut self) {
        self.items.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Succeeded,
    Failed,
    Denied,
    /// The operation may or may not have taken effect.
    Uncertain,
}

/// One record per intervention initiated through the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: u64,
    pub at: u64,
    pub profile: String,
    pub method: String,
    pub effect: crate::safety::Effect,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<crate::safety::Resource>,
    pub outcome: AuditOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Redacted request parameters.
    pub params: Value,
}

#[derive(Debug)]
pub struct AuditLog {
    buf: BoundedBuffer<AuditRecord>,
    seq: u64,
    sink: Option<std::fs::File>,
}

impl AuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BoundedBuffer::new(capacity),
            seq: 0,
            sink: None,
        }
    }

    /// Additionally append JSON lines to `path`.
    pub fn with_file(mut self, path: &std::path::Path) -> std::io::Result<Self> {
        let f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        self.sink = Some(f);
        Ok(self)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        profile: &str,
        method: &str,
        effect: crate::safety::Effect,
        resource: Option<crate::safety::Resource>,
        outcome: AuditOutcome,
        error: Option<String>,
        params: &Value,
    ) -> AuditRecord {
        self.seq += 1;
        let rec = AuditRecord {
            seq: self.seq,
            at: crate::message::now_millis(),
            profile: profile.into(),
            method: method.into(),
            effect,
            resource,
            outcome,
            error: error.map(|e| crate::redact::redact_text(&e)),
            params: strip_payloads(&redact_json(params)),
        };
        if let Some(f) = &mut self.sink {
            use std::io::Write;
            if let Ok(line) = serde_json::to_string(&rec) {
                let _ = writeln!(f, "{line}");
            }
        }
        self.buf.push(rec.clone());
        rec
    }

    pub fn recent(&self, max: usize) -> Vec<AuditRecord> {
        let skip = self.buf.len().saturating_sub(max);
        self.buf.iter().skip(skip).cloned().collect()
    }
}

/// Message bodies are not audit material; keep only their size.
fn strip_payloads(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| {
                    if k == "payload" {
                        let size = v.get("data").and_then(Value::as_str).map(str::len).unwrap_or(0);
                        (k.clone(), serde_json::json!({ "omitted": true, "encoded_size": size }))
                    } else {
                        (k.clone(), strip_payloads(v))
                    }
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(strip_payloads).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safety::Effect;
    use serde_json::json;

    #[test]
    fn bounded() {
        let mut b = BoundedBuffer::new(2);
        assert!(b.push(1).is_none());
        b.push(2);
        assert_eq!(b.push(3), Some(1));
        assert_eq!(b.dropped(), 1);
        assert_eq!(b.total(), 3);
        assert_eq!(b.drain(10), vec![2, 3]);
        assert!(b.is_empty());
    }

    #[test]
    fn audit_redacts_and_strips() {
        let mut a = AuditLog::new(10);
        let r = a.record(
            "p",
            "queue.delete",
            Effect::Destructive,
            None,
            AuditOutcome::Succeeded,
            None,
            &json!({"password": "x", "messages": [{"payload": {"data": "secret body"}}]}),
        );
        assert_eq!(r.params["password"], crate::redact::MASK);
        assert_eq!(r.params["messages"][0]["payload"]["omitted"], true);
        assert_eq!(a.recent(5).len(), 1);
    }
}
