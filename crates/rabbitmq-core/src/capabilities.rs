//! Queue-type and broker-version capability gating.
//!
//! RabbitMQ rejects many argument combinations at declare time with a 406
//! and a terse message. Validating locally first lets the UI explain *why*
//! before anything is sent, and keeps the rules documented in one place.
//! The broker remains the authority: local validation only rejects
//! combinations that RabbitMQ 4.4 itself rejects (verified against a live
//! 4.4.0 node; see `tests/integration` in rabbitmq-bridge).

use crate::error::{CoreError, ErrorCode, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueType {
    Classic,
    Quorum,
    Stream,
}

impl QueueType {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "classic" => Ok(Self::Classic),
            "quorum" => Ok(Self::Quorum),
            "stream" => Ok(Self::Stream),
            other => Err(CoreError::invalid(format!(
                "unknown queue type \"{other}\" (expected classic, quorum, or stream)"
            ))),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Quorum => "quorum",
            Self::Stream => "stream",
        }
    }

    /// Whether `basic.get` is permitted.
    pub fn supports_basic_get(self) -> bool {
        self != Self::Stream
    }
    /// Whether consumers may use automatic acknowledgement.
    pub fn supports_auto_ack(self) -> bool {
        self != Self::Stream
    }
    /// Whether the queue type supports non-destructive offset-based reads.
    pub fn supports_replay(self) -> bool {
        self == Self::Stream
    }
    pub fn supports_purge(self) -> bool {
        self != Self::Stream
    }
}

/// Parsed broker version (`major.minor.patch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BrokerVersion(pub u32, pub u32, pub u32);

impl BrokerVersion {
    pub fn parse(s: &str) -> Option<Self> {
        let mut it = s
            .split(|c: char| !c.is_ascii_digit())
            .filter(|p| !p.is_empty())
            .map(|p| p.parse::<u32>().ok());
        Some(Self(it.next()??, it.next().flatten().unwrap_or(0), it.next().flatten().unwrap_or(0)))
    }
    pub fn at_least(self, major: u32, minor: u32) -> bool {
        (self.0, self.1) >= (major, minor)
    }
}

/// Feature availability by broker version. Used to gate UI commands and to
/// produce `unsupported` errors with an explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureMatrix {
    pub version: BrokerVersion,
    pub native_amqp10: bool,
    pub stream_filtering: bool,
    pub quorum_priorities: bool,
    pub khepri_capable: bool,
    pub classic_mirroring: bool,
    pub stream_sac: bool,
}

impl FeatureMatrix {
    pub fn for_version(v: BrokerVersion) -> Self {
        Self {
            version: v,
            // AMQP 1.0 became a core protocol in 4.0.
            native_amqp10: v.at_least(4, 0),
            // Stream filtering arrived in 3.13.
            stream_filtering: v.at_least(3, 13),
            // Quorum queue message priorities in 4.0.
            quorum_priorities: v.at_least(4, 0),
            khepri_capable: v.at_least(3, 13),
            // Classic queue mirroring was removed in 4.0.
            classic_mirroring: !v.at_least(4, 0),
            stream_sac: v.at_least(3, 11),
        }
    }
}

struct ArgRule {
    name: &'static str,
    classic: bool,
    quorum: bool,
    stream: bool,
    note: &'static str,
}

const ARGS: &[ArgRule] = &[
    ArgRule { name: "x-message-ttl", classic: true, quorum: true, stream: false, note: "streams use x-max-age retention instead" },
    ArgRule { name: "x-expires", classic: true, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-max-length", classic: true, quorum: true, stream: false, note: "streams use x-max-length-bytes retention" },
    ArgRule { name: "x-max-length-bytes", classic: true, quorum: true, stream: true, note: "" },
    ArgRule { name: "x-overflow", classic: true, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-dead-letter-exchange", classic: true, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-dead-letter-routing-key", classic: true, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-dead-letter-strategy", classic: false, quorum: true, stream: false, note: "at-least-once dead-lettering is quorum-only" },
    ArgRule { name: "x-max-priority", classic: true, quorum: false, stream: false, note: "quorum queues support priorities without this argument (4.0+)" },
    ArgRule { name: "x-queue-mode", classic: true, quorum: false, stream: false, note: "deprecated; classic queues v2 ignore it" },
    ArgRule { name: "x-queue-version", classic: true, quorum: false, stream: false, note: "" },
    ArgRule { name: "x-single-active-consumer", classic: true, quorum: true, stream: false, note: "streams use per-consumer single-active-consumer" },
    ArgRule { name: "x-delivery-limit", classic: false, quorum: true, stream: false, note: "poison-message handling is quorum-only" },
    ArgRule { name: "x-quorum-initial-group-size", classic: false, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-quorum-target-group-size", classic: false, quorum: true, stream: false, note: "" },
    ArgRule { name: "x-max-in-memory-length", classic: false, quorum: true, stream: false, note: "accepted but ignored since RabbitMQ 4.0" },
    ArgRule { name: "x-max-in-memory-bytes", classic: false, quorum: true, stream: false, note: "accepted but ignored since RabbitMQ 4.0" },
    ArgRule { name: "x-max-age", classic: false, quorum: false, stream: true, note: "" },
    ArgRule { name: "x-stream-max-segment-size-bytes", classic: false, quorum: false, stream: true, note: "" },
    ArgRule { name: "x-stream-filter-size-bytes", classic: false, quorum: false, stream: true, note: "" },
    ArgRule { name: "x-initial-cluster-size", classic: false, quorum: false, stream: true, note: "" },
    ArgRule { name: "x-queue-leader-locator", classic: false, quorum: true, stream: true, note: "" },
    ArgRule { name: "x-queue-master-locator", classic: true, quorum: false, stream: false, note: "deprecated; use x-queue-leader-locator" },
];

/// A queue declaration request after validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueSpec {
    pub name: String,
    pub queue_type: QueueType,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: Map<String, Value>,
}

impl QueueSpec {
    /// Validate a declaration for the target queue type. Also injects
    /// `x-queue-type` so the broker creates the intended type.
    pub fn validate(mut self) -> Result<Self> {
        let qt = self.queue_type;
        if self.name.is_empty() && qt != QueueType::Classic {
            return Err(CoreError::invalid(format!(
                "{} queues must be named; server-named queues are classic only",
                qt.as_str()
            )));
        }
        if self.name.starts_with("amq.") {
            return Err(CoreError::new(
                ErrorCode::AccessRefused,
                "queue names starting with \"amq.\" are reserved by the broker",
            ));
        }
        if qt != QueueType::Classic {
            if !self.durable {
                return Err(CoreError::invalid(format!("{} queues must be durable", qt.as_str())));
            }
            if self.exclusive {
                return Err(CoreError::invalid(format!("{} queues cannot be exclusive", qt.as_str())));
            }
            if self.auto_delete {
                return Err(CoreError::invalid(format!("{} queues cannot be auto-delete", qt.as_str())));
            }
        }
        for (k, _) in self.arguments.iter() {
            if k == "x-queue-type" {
                continue;
            }
            if let Some(rule) = ARGS.iter().find(|r| r.name == k) {
                let ok = match qt {
                    QueueType::Classic => rule.classic,
                    QueueType::Quorum => rule.quorum,
                    QueueType::Stream => rule.stream,
                };
                if !ok {
                    let mut msg = format!("argument {k} is not supported by {} queues", qt.as_str());
                    if !rule.note.is_empty() {
                        msg.push_str(&format!(" ({})", rule.note));
                    }
                    return Err(CoreError::unsupported(msg));
                }
            }
        }
        if let Some(v) = self.arguments.get("x-overflow").and_then(Value::as_str) {
            let allowed: &[&str] = &["drop-head", "reject-publish", "reject-publish-dlx"];
            if !allowed.contains(&v) {
                return Err(CoreError::unsupported(format!(
                    "x-overflow={v} is not supported by {} queues (allowed: {})",
                    qt.as_str(),
                    allowed.join(", ")
                )));
            }
        }
        if let Some(v) = self.arguments.get("x-max-priority") {
            match v.as_u64() {
                Some(1..=255) => {}
                _ => return Err(CoreError::invalid("x-max-priority must be an integer between 1 and 255")),
            }
        }
        if let Some(v) = self.arguments.get("x-max-age").and_then(Value::as_str) {
            if !valid_max_age(v) {
                return Err(CoreError::invalid(
                    "x-max-age must be a number followed by Y, M, D, h, m, or s (e.g. 7D)",
                ));
            }
        }
        self.arguments
            .insert("x-queue-type".into(), Value::String(qt.as_str().into()));
        Ok(self)
    }

    /// Non-fatal advisories: combinations the broker may reject depending on
    /// configuration, or that are deprecated.
    pub fn warnings(&self) -> Vec<String> {
        let mut w = Vec::new();
        if self.queue_type == QueueType::Classic && !self.durable && !self.exclusive {
            w.push("transient non-exclusive queues are a deprecated feature; RabbitMQ 4.x rejects them unless deprecated_features.permit.transient_nonexcl_queues = true".into());
        }
        for k in ["x-max-in-memory-length", "x-max-in-memory-bytes", "x-queue-mode", "x-queue-master-locator"] {
            if self.arguments.contains_key(k) {
                w.push(format!("{k} is deprecated or ignored by RabbitMQ 4.x"));
            }
        }
        w
    }
}

fn valid_max_age(v: &str) -> bool {
    let (num, unit) = v.split_at(v.len().saturating_sub(1));
    !num.is_empty() && num.chars().all(|c| c.is_ascii_digit()) && matches!(unit, "Y" | "M" | "D" | "h" | "m" | "s")
}

/// Look up the queue type of a declared queue from its arguments or
/// Management API `type` field.
pub fn queue_type_of(kind: Option<&str>, arguments: &Map<String, Value>) -> QueueType {
    kind.or_else(|| arguments.get("x-queue-type").and_then(Value::as_str))
        .and_then(|s| QueueType::parse(s).ok())
        .unwrap_or(QueueType::Classic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(qt: QueueType, args: Value) -> QueueSpec {
        QueueSpec {
            name: "q".into(),
            queue_type: qt,
            durable: true,
            exclusive: false,
            auto_delete: false,
            arguments: args.as_object().unwrap().clone(),
        }
    }

    #[test]
    fn version_parse() {
        assert_eq!(BrokerVersion::parse("4.4.0"), Some(BrokerVersion(4, 4, 0)));
        assert_eq!(BrokerVersion::parse("3.13.7+1.g1"), Some(BrokerVersion(3, 13, 7)));
        assert_eq!(BrokerVersion::parse("4"), Some(BrokerVersion(4, 0, 0)));
        assert!(BrokerVersion::parse("abc").is_none());
        let f = FeatureMatrix::for_version(BrokerVersion(4, 4, 0));
        assert!(f.native_amqp10 && !f.classic_mirroring);
        assert!(FeatureMatrix::for_version(BrokerVersion(3, 12, 0)).classic_mirroring);
    }

    #[test]
    fn quorum_rules() {
        assert!(spec(QueueType::Quorum, json!({"x-delivery-limit": 5})).validate().is_ok());
        let e = spec(QueueType::Quorum, json!({"x-max-priority": 5})).validate().unwrap_err();
        assert_eq!(e.code, ErrorCode::Unsupported);
        let mut s = spec(QueueType::Quorum, json!({}));
        s.exclusive = true;
        assert!(s.validate().is_err());
        assert!(spec(QueueType::Quorum, json!({"x-overflow": "reject-publish-dlx"})).validate().is_ok());
        assert!(spec(QueueType::Quorum, json!({"x-overflow": "bogus"})).validate().is_err());
        assert!(spec(QueueType::Quorum, json!({"x-queue-mode": "lazy"})).validate().is_err());
    }

    #[test]
    fn stream_rules() {
        let ok = spec(QueueType::Stream, json!({"x-max-age": "7D", "x-max-length-bytes": 1000})).validate().unwrap();
        assert_eq!(ok.arguments["x-queue-type"], "stream");
        assert!(spec(QueueType::Stream, json!({"x-message-ttl": 5})).validate().is_err());
        assert!(spec(QueueType::Stream, json!({"x-max-age": "7days"})).validate().is_err());
        assert!(!QueueType::Stream.supports_basic_get());
    }

    #[test]
    fn classic_rules() {
        assert!(spec(QueueType::Classic, json!({"x-max-priority": 10})).validate().is_ok());
        assert!(spec(QueueType::Classic, json!({"x-max-priority": 0})).validate().is_err());
        assert!(spec(QueueType::Classic, json!({"x-delivery-limit": 3})).validate().is_err());
        let mut s = spec(QueueType::Classic, json!({}));
        s.name = "amq.foo".into();
        assert!(s.clone().validate().is_err());
        s.name = String::new();
        s.durable = false;
        s.exclusive = true;
        assert!(s.clone().validate().is_ok());
        assert!(s.warnings().is_empty());
        s.exclusive = false;
        assert_eq!(s.warnings().len(), 1);
    }
}
