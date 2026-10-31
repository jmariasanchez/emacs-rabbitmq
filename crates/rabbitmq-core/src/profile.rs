//! Connection profiles.
//!
//! A profile names one RabbitMQ deployment and carries everything needed to
//! reach its AMQP 0-9-1, AMQP 1.0, Stream, and Management endpoints, plus the
//! safety policy and resource limits that apply to it. Profiles are supplied
//! by the frontend (Emacs resolves credentials through `auth-source`) and are
//! never written to disk by the bridge.

use crate::safety::{ProtectedRule, SafetyPolicy};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// A secret string. Never printed by `Debug`, never serialized in clear.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    /// Access the clear value. Callers must not log the result.
    pub fn expose(&self) -> &str {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(crate::redact::MASK)
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(crate::redact::MASK)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Secret(String::deserialize(d)?))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Use TLS for AMQP/stream connections (`amqps`).
    #[serde(default)]
    pub enabled: bool,
    /// PEM file with trusted CA certificates. When absent the platform /
    /// webpki roots are used.
    #[serde(default)]
    pub ca_file: Option<String>,
    /// PEM client certificate (for mutual TLS / EXTERNAL auth).
    #[serde(default)]
    pub client_cert_file: Option<String>,
    /// PEM private key for the client certificate.
    #[serde(default)]
    pub client_key_file: Option<String>,
    /// PKCS#12 bundle as an alternative to cert+key (AMQP 0-9-1 adapter).
    #[serde(default)]
    pub pkcs12_file: Option<String>,
    #[serde(default)]
    pub pkcs12_password: Option<Secret>,
    /// Verify the server certificate. Disabling is only for local testing and
    /// is surfaced prominently in the UI.
    #[serde(default = "yes")]
    pub verify: bool,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMechanism {
    #[default]
    Plain,
    Amqplain,
    External,
}

/// Resource limits protecting the bridge and Emacs from unbounded growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum deliveries buffered per consumer before the consumer is
    /// paused (backpressure via prefetch) and older entries are dropped.
    #[serde(default = "d_buffer")]
    pub max_buffered_deliveries: usize,
    /// Maximum bytes of payload sent to Emacs per message; the rest is
    /// available on demand and marked `truncated`.
    #[serde(default = "d_preview")]
    pub max_payload_preview: usize,
    /// Maximum concurrent consumers per session.
    #[serde(default = "d_consumers")]
    pub max_consumers: usize,
    /// Maximum outstanding (unsettled) deliveries tracked per session.
    #[serde(default = "d_outstanding")]
    pub max_unsettled: usize,
    /// Maximum messages per publish batch.
    #[serde(default = "d_batch")]
    pub max_publish_batch: usize,
    /// Maximum messages per pull (basic.get / stream read) request.
    #[serde(default = "d_pull")]
    pub max_pull: usize,
    /// Default operation timeout in milliseconds.
    #[serde(default = "d_timeout")]
    pub timeout_ms: u64,
}

fn d_buffer() -> usize {
    1000
}
fn d_preview() -> usize {
    64 * 1024
}
fn d_consumers() -> usize {
    32
}
fn d_outstanding() -> usize {
    5000
}
fn d_batch() -> usize {
    10_000
}
fn d_pull() -> usize {
    500
}
fn d_timeout() -> u64 {
    15_000
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_buffered_deliveries: d_buffer(),
            max_payload_preview: d_preview(),
            max_consumers: d_consumers(),
            max_unsettled: d_outstanding(),
            max_publish_batch: d_batch(),
            max_pull: d_pull(),
            timeout_ms: d_timeout(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    #[serde(default = "d_host")]
    pub host: String,
    #[serde(default = "d_amqp_port")]
    pub amqp_port: u16,
    /// AMQP 1.0 port; RabbitMQ 4.x serves AMQP 1.0 on the same listener as 0-9-1.
    #[serde(default)]
    pub amqp10_port: Option<u16>,
    #[serde(default = "d_stream_port")]
    pub stream_port: u16,
    /// Management API base URL, e.g. `http://localhost:15672`.
    #[serde(default)]
    pub management_url: Option<String>,
    #[serde(default = "d_vhost")]
    pub vhost: String,
    #[serde(default = "d_user")]
    pub username: String,
    #[serde(default)]
    pub password: Secret,
    #[serde(default)]
    pub auth_mechanism: AuthMechanism,
    #[serde(default)]
    pub tls: TlsSettings,
    /// Heartbeat timeout in seconds (0 disables).
    #[serde(default = "d_heartbeat")]
    pub heartbeat: u16,
    /// Client-provided connection name shown in the management UI.
    #[serde(default)]
    pub connection_name: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub protected: Vec<ProtectedRule>,
    #[serde(default)]
    pub limits: Limits,
    /// Maximum automatic reconnect attempts (0 disables). Reconnection never
    /// replays ambiguous operations.
    #[serde(default = "d_reconnect")]
    pub reconnect_attempts: u32,
}

fn d_host() -> String {
    "localhost".into()
}
fn d_amqp_port() -> u16 {
    5672
}
fn d_stream_port() -> u16 {
    5552
}
fn d_vhost() -> String {
    "/".into()
}
fn d_user() -> String {
    "guest".into()
}
fn d_heartbeat() -> u16 {
    30
}
fn d_reconnect() -> u32 {
    5
}

impl Profile {
    pub fn local(name: &str) -> Self {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "password": "guest",
            "management_url": "http://localhost:15672"
        }))
        .expect("valid default profile")
    }

    pub fn safety(&self) -> SafetyPolicy {
        SafetyPolicy {
            read_only: self.read_only,
            protected: self.protected.clone(),
        }
    }

    pub fn scheme(&self) -> &'static str {
        if self.tls.enabled {
            "amqps"
        } else {
            "amqp"
        }
    }

    /// AMQP URI including credentials. Callers must not log it; use
    /// [`Profile::display_uri`] for display.
    pub fn amqp_uri(&self) -> String {
        format!(
            "{}://{}:{}@{}:{}/{}?heartbeat={}",
            self.scheme(),
            pct(&self.username),
            pct(self.password.expose()),
            self.host,
            self.amqp_port,
            pct(&self.vhost),
            self.heartbeat
        ) + match self.auth_mechanism {
            AuthMechanism::Plain => "",
            AuthMechanism::Amqplain => "&auth_mechanism=amqplain",
            AuthMechanism::External => "&auth_mechanism=external",
        }
    }

    pub fn display_uri(&self) -> String {
        format!(
            "{}://{}@{}:{}/{}",
            self.scheme(),
            self.username,
            self.host,
            self.amqp_port,
            pct(&self.vhost)
        )
    }

    pub fn connection_name(&self) -> String {
        self.connection_name
            .clone()
            .unwrap_or_else(|| format!("rabbitmq.el/{}", self.name))
    }
}

/// Percent-encode everything outside the RFC 3986 unreserved set.
pub fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_uri() {
        let p = Profile::local("dev");
        assert_eq!(p.vhost, "/");
        assert_eq!(p.amqp_uri(), "amqp://guest:guest@localhost:5672/%2F?heartbeat=30");
        assert_eq!(p.display_uri(), "amqp://guest@localhost:5672/%2F");
        assert_eq!(p.limits, Limits::default());
    }

    #[test]
    fn secret_never_serialized() {
        let p = Profile::local("dev");
        let s = serde_json::to_string(&p).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["password"], crate::redact::MASK, "{s}");
        assert_eq!(format!("{:?}", p.password), crate::redact::MASK);
    }

    #[test]
    fn special_chars_encoded() {
        let mut p = Profile::local("x");
        p.password = Secret::new("p@ss:w/rd");
        assert!(p.amqp_uri().contains("p%40ss%3Aw%2Frd"));
    }
}
