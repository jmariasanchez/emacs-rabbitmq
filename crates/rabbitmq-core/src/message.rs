//! Protocol-neutral message model used across the RPC boundary.
//!
//! Payloads cross the boundary as `{encoding, data}` so binary content is
//! never mangled by JSON. Inbound payloads larger than the profile's preview
//! limit are truncated and flagged; the full size is always reported.

use crate::error::{CoreError, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadEncoding {
    /// UTF-8 text.
    #[default]
    Utf8,
    /// Arbitrary bytes, base64 encoded.
    Base64,
    /// Lowercase hexadecimal.
    Hex,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Payload {
    #[serde(default)]
    pub encoding: PayloadEncoding,
    pub data: String,
}

impl Payload {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            encoding: PayloadEncoding::Utf8,
            data: s.into(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self.encoding {
            PayloadEncoding::Utf8 => Ok(self.data.as_bytes().to_vec()),
            PayloadEncoding::Base64 => B64
                .decode(self.data.trim())
                .map_err(|e| CoreError::invalid(format!("invalid base64 payload: {e}"))),
            PayloadEncoding::Hex => decode_hex(&self.data),
        }
    }

    /// Encode bytes for transport to the frontend: UTF-8 when valid,
    /// otherwise base64.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(s) => Self::text(s),
            Err(_) => Self {
                encoding: PayloadEncoding::Base64,
                data: B64.encode(bytes),
            },
        }
    }
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if !clean.is_ascii() {
        return Err(CoreError::invalid("invalid hex payload"));
    }
    if clean.len() % 2 != 0 {
        return Err(CoreError::invalid("hex payload has odd length"));
    }
    (0..clean.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&clean[i..i + 2], 16)
                .map_err(|_| CoreError::invalid("invalid hex payload"))
        })
        .collect()
}

pub fn encode_b64(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

pub fn decode_b64(s: &str) -> Result<Vec<u8>> {
    B64.decode(s)
        .map_err(|e| CoreError::invalid(format!("invalid base64: {e}")))
}

/// Inspected payload sent to Emacs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadView {
    pub encoding: PayloadEncoding,
    pub data: String,
    /// Total payload size in bytes.
    pub size: usize,
    /// True when `data` holds only the first `max_preview` bytes.
    pub truncated: bool,
    /// Whether the full payload is valid UTF-8 (independent of truncation).
    pub utf8: bool,
}

impl PayloadView {
    pub fn new(bytes: &[u8], max_preview: usize) -> Self {
        let utf8 = std::str::from_utf8(bytes).is_ok();
        let truncated = bytes.len() > max_preview;
        let mut slice = &bytes[..bytes.len().min(max_preview)];
        if utf8 && truncated {
            // Do not cut a code point in half.
            while !slice.is_empty() && std::str::from_utf8(slice).is_err() {
                slice = &slice[..slice.len() - 1];
            }
        }
        let p = if utf8 {
            Payload::text(String::from_utf8_lossy(slice).into_owned())
        } else {
            Payload::from_bytes(slice)
        };
        Self {
            encoding: p.encoding,
            data: p.data,
            size: bytes.len(),
            truncated,
            utf8,
        }
    }
}

/// AMQP 0-9-1 basic properties in a protocol-neutral shape. AMQP 1.0 and
/// stream messages map their properties/application-properties onto the
/// same fields where semantics match; the rest goes into `annotations`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MessageProperties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    /// Headers / application properties. Typed values use `{"$type": ..., "value": ...}`.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub headers: Map<String, Value>,
    /// 1 = transient, 2 = persistent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_mode: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    /// Per-message TTL in milliseconds (string per AMQP 0-9-1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Seconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// AMQP 1.0 / stream message annotations and other protocol-specific fields.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub annotations: Map<String, Value>,
}

impl MessageProperties {
    pub fn validate(&self) -> Result<()> {
        if let Some(m) = self.delivery_mode {
            if m != 1 && m != 2 {
                return Err(CoreError::invalid(
                    "delivery_mode must be 1 (transient) or 2 (persistent)",
                ));
            }
        }
        if let Some(e) = &self.expiration {
            if e.parse::<u64>().is_err() {
                return Err(CoreError::invalid(
                    "expiration must be a non-negative integer number of milliseconds",
                ));
            }
        }
        Ok(())
    }
}

/// A message to publish.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct OutboundMessage {
    #[serde(default)]
    pub exchange: String,
    #[serde(default)]
    pub routing_key: String,
    pub payload: Payload,
    #[serde(default)]
    pub properties: MessageProperties,
    /// Ask the broker to return the message if it cannot be routed.
    #[serde(default)]
    pub mandatory: bool,
}

/// Classified publish outcome. Socket writes are never reported as success:
/// only a broker confirm yields `Confirmed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishStatus {
    /// Broker positively confirmed (publisher confirms enabled).
    Confirmed,
    /// Broker returned the message as unroutable (mandatory) and confirmed the return.
    Returned,
    /// Broker negatively confirmed (internal error, queue length overflow with reject-publish, ...).
    Nacked,
    /// Written to the socket without confirms; broker acceptance unknown.
    Unconfirmed,
    /// Connection/channel failed before a confirm arrived: the broker may or may not have the message.
    Uncertain,
    /// Rejected before reaching the broker (validation, policy).
    Failed,
    /// Committed inside an AMQP transaction.
    Committed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishOutcome {
    pub index: usize,
    pub status: PublishStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PublishSummary {
    pub confirmed: usize,
    pub returned: usize,
    pub nacked: usize,
    pub unconfirmed: usize,
    pub uncertain: usize,
    pub failed: usize,
    pub committed: usize,
}

impl PublishSummary {
    pub fn from_outcomes(o: &[PublishOutcome]) -> Self {
        let mut s = Self::default();
        for x in o {
            match x.status {
                PublishStatus::Confirmed => s.confirmed += 1,
                PublishStatus::Returned => s.returned += 1,
                PublishStatus::Nacked => s.nacked += 1,
                PublishStatus::Unconfirmed => s.unconfirmed += 1,
                PublishStatus::Uncertain => s.uncertain += 1,
                PublishStatus::Failed => s.failed += 1,
                PublishStatus::Committed => s.committed += 1,
            }
        }
        s
    }
}

/// Which protocol path produced an inbound message, and how it may be settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliverySource {
    /// AMQP 0-9-1 consumer or basic.get. `delivery_tag` is only valid on
    /// `channel` of `connection_epoch`; the bridge rejects stale settlements.
    Amqp091 {
        queue: String,
        channel: u16,
        delivery_tag: u64,
        connection_epoch: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        consumer_tag: Option<String>,
        /// True when the delivery was auto-acknowledged and cannot be settled.
        auto_ack: bool,
    },
    /// Native stream read. No settlement: streams are non-destructive.
    Stream {
        stream: String,
        offset: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chunk_timestamp: Option<i64>,
    },
    /// AMQP 1.0 receiver link delivery.
    Amqp10 {
        address: String,
        link: String,
        delivery_id: u32,
        settled: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboundMessage {
    /// Bridge-assigned id, unique per bridge process. Used for settlement.
    pub id: String,
    pub source: DeliverySource,
    #[serde(default)]
    pub exchange: String,
    #[serde(default)]
    pub routing_key: String,
    pub redelivered: bool,
    pub properties: MessageProperties,
    pub payload: PayloadView,
    /// Milliseconds since epoch when the bridge received it.
    pub received_at: u64,
    /// Remaining messages reported by basic.get-ok, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Settlement {
    Ack,
    /// Negative acknowledgement; `requeue` decides whether the broker
    /// requeues or dead-letters/drops.
    Nack {
        requeue: bool,
    },
    Reject {
        requeue: bool,
    },
    /// AMQP 1.0 `released` outcome.
    Release,
    /// AMQP 1.0 `modified` outcome.
    Modify {
        delivery_failed: bool,
        undeliverable_here: bool,
    },
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_roundtrip() {
        let bin = vec![0u8, 159, 146, 150, 255];
        let p = Payload::from_bytes(&bin);
        assert_eq!(p.encoding, PayloadEncoding::Base64);
        assert_eq!(p.to_bytes().unwrap(), bin);
        let t = Payload::from_bytes("héllo".as_bytes());
        assert_eq!(t.encoding, PayloadEncoding::Utf8);
        let h = Payload {
            encoding: PayloadEncoding::Hex,
            data: "00 ff 10".into(),
        };
        assert_eq!(h.to_bytes().unwrap(), vec![0, 255, 16]);
        assert!(Payload {
            encoding: PayloadEncoding::Hex,
            data: "0".into()
        }
        .to_bytes()
        .is_err());
    }

    #[test]
    fn non_ascii_hex_is_an_error_not_a_panic() {
        for data in ["a€", "éé", "🙂"] {
            assert!(Payload {
                encoding: PayloadEncoding::Hex,
                data: data.into()
            }
            .to_bytes()
            .is_err());
        }
    }

    #[test]
    fn preview_truncation_respects_utf8() {
        let s = "aé".repeat(10); // é is 2 bytes
        let v = PayloadView::new(s.as_bytes(), 4);
        assert!(v.truncated && v.utf8);
        assert_eq!(v.size, 30);
        assert_eq!(v.data, "aéa");
        let bin = PayloadView::new(&[0xff; 10], 3);
        assert_eq!(bin.encoding, PayloadEncoding::Base64);
        assert_eq!(decode_b64(&bin.data).unwrap().len(), 3);
    }

    #[test]
    fn property_validation() {
        let mut p = MessageProperties::default();
        assert!(p.validate().is_ok());
        p.delivery_mode = Some(3);
        assert!(p.validate().is_err());
        p.delivery_mode = Some(2);
        p.expiration = Some("abc".into());
        assert!(p.validate().is_err());
    }
}
