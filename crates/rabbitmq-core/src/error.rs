//! Structured error model shared by every adapter.
//!
//! Every error carries a stable machine-readable [`ErrorCode`], a human
//! message, and two safety-relevant flags:
//!
//! * `retryable`: the operation can be retried without risk of duplicating
//!   an effect (for example a failed passive declare).
//! * `ambiguous`: the broker may or may not have applied the operation. The
//!   client must never retry an ambiguous destructive operation automatically.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Stable error codes. These are part of the RPC contract (see `docs/protocol/RPC.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Malformed parameters supplied by the caller.
    InvalidParams,
    /// Named session, consumer, delivery, or profile does not exist.
    NotFound,
    /// Network or TLS failure before or during an operation.
    ConnectionFailed,
    /// Authentication rejected by the broker.
    AuthenticationFailed,
    /// Broker refused the operation because of permissions (AMQP 403 / HTTP 401/403).
    AccessRefused,
    /// Broker reported a precondition failure (AMQP 406), e.g. a declare
    /// with arguments that differ from the existing entity.
    PreconditionFailed,
    /// Entity is held exclusively by another connection (AMQP 405).
    ResourceLocked,
    /// The profile is read-only and the operation would alter broker state.
    ReadOnlyProfile,
    /// The target matches a protected-resource rule.
    ProtectedResource,
    /// A destructive operation was requested without the explicit confirmation token.
    ConfirmationRequired,
    /// Operation not supported for this queue type, protocol, or broker version.
    Unsupported,
    /// Channel or connection was closed; delivery tags from it are invalid.
    ChannelClosed,
    /// Delivery tag is stale (its channel has been replaced).
    StaleDelivery,
    /// Configured resource limit reached (buffers, consumers, outstanding requests).
    LimitExceeded,
    /// Operation exceeded its deadline.
    Timeout,
    /// Request was cancelled by the client.
    Cancelled,
    /// Error returned by the Management HTTP API that does not map to another code.
    ManagementApi,
    /// Generic broker-side protocol error.
    Protocol,
    /// Unexpected internal failure.
    Internal,
}

impl ErrorCode {
    /// Numeric JSON-RPC error code. Application codes live in -32000..-32099.
    pub fn rpc_code(self) -> i64 {
        match self {
            ErrorCode::InvalidParams => -32602,
            ErrorCode::NotFound => -32001,
            ErrorCode::ConnectionFailed => -32002,
            ErrorCode::AuthenticationFailed => -32003,
            ErrorCode::AccessRefused => -32004,
            ErrorCode::PreconditionFailed => -32005,
            ErrorCode::ResourceLocked => -32006,
            ErrorCode::ReadOnlyProfile => -32007,
            ErrorCode::ProtectedResource => -32008,
            ErrorCode::ConfirmationRequired => -32009,
            ErrorCode::Unsupported => -32010,
            ErrorCode::ChannelClosed => -32011,
            ErrorCode::StaleDelivery => -32012,
            ErrorCode::LimitExceeded => -32013,
            ErrorCode::Timeout => -32014,
            ErrorCode::Cancelled => -32800,
            ErrorCode::ManagementApi => -32015,
            ErrorCode::Protocol => -32016,
            ErrorCode::Internal => -32603,
        }
    }

    pub fn as_str(self) -> &'static str {
        // Must stay in sync with the serde names; checked by `rpc_codes_are_unique`.
        match self {
            ErrorCode::InvalidParams => "invalid_params",
            ErrorCode::NotFound => "not_found",
            ErrorCode::ConnectionFailed => "connection_failed",
            ErrorCode::AuthenticationFailed => "authentication_failed",
            ErrorCode::AccessRefused => "access_refused",
            ErrorCode::PreconditionFailed => "precondition_failed",
            ErrorCode::ResourceLocked => "resource_locked",
            ErrorCode::ReadOnlyProfile => "read_only_profile",
            ErrorCode::ProtectedResource => "protected_resource",
            ErrorCode::ConfirmationRequired => "confirmation_required",
            ErrorCode::Unsupported => "unsupported",
            ErrorCode::ChannelClosed => "channel_closed",
            ErrorCode::StaleDelivery => "stale_delivery",
            ErrorCode::LimitExceeded => "limit_exceeded",
            ErrorCode::Timeout => "timeout",
            ErrorCode::Cancelled => "cancelled",
            ErrorCode::ManagementApi => "management_api",
            ErrorCode::Protocol => "protocol",
            ErrorCode::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error type returned by all rabbitmq-* crates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct CoreError {
    pub code: ErrorCode,
    pub message: String,
    /// Safe to retry without duplicating effects.
    #[serde(default)]
    pub retryable: bool,
    /// The broker may have applied the operation. Never auto-retry.
    #[serde(default)]
    pub ambiguous: bool,
    /// Optional actionable hint for the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Optional structured detail (broker reply code, HTTP status, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl CoreError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: crate::redact::redact_text(&message.into()),
            retryable: false,
            ambiguous: false,
            hint: None,
            detail: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidParams, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unsupported, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    pub fn retryable(mut self, yes: bool) -> Self {
        self.retryable = yes;
        self
    }

    pub fn ambiguous(mut self, yes: bool) -> Self {
        self.ambiguous = yes;
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }

    /// Map an AMQP 0-9-1 reply code (from channel.close / connection.close)
    /// to an error with an actionable hint.
    pub fn from_amqp_reply(reply_code: u16, text: &str) -> Self {
        let (code, hint) = match reply_code {
            403 => (
                ErrorCode::AccessRefused,
                Some("The user lacks configure/write/read permission on this resource in the virtual host."),
            ),
            404 => (ErrorCode::NotFound, None),
            405 => (
                ErrorCode::ResourceLocked,
                Some("The resource is exclusive to another connection."),
            ),
            406 => (
                ErrorCode::PreconditionFailed,
                Some("An entity with the same name exists with different properties or arguments. Inspect it and use the same settings, or delete it explicitly."),
            ),
            320 => (
                ErrorCode::ConnectionFailed,
                Some("The broker forced the connection closed (often a node shutdown or an operator action)."),
            ),
            530 | 540 => (ErrorCode::Unsupported, None),
            _ => (ErrorCode::Protocol, None),
        };
        let mut e = Self::new(code, format!("broker replied {reply_code}: {text}"))
            .with_detail(serde_json::json!({ "reply_code": reply_code }));
        if let Some(h) = hint {
            e = e.with_hint(h);
        }
        e
    }
}

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_codes_are_unique() {
        use std::collections::HashSet;
        let all = [
            ErrorCode::InvalidParams,
            ErrorCode::NotFound,
            ErrorCode::ConnectionFailed,
            ErrorCode::AuthenticationFailed,
            ErrorCode::AccessRefused,
            ErrorCode::PreconditionFailed,
            ErrorCode::ResourceLocked,
            ErrorCode::ReadOnlyProfile,
            ErrorCode::ProtectedResource,
            ErrorCode::ConfirmationRequired,
            ErrorCode::Unsupported,
            ErrorCode::ChannelClosed,
            ErrorCode::StaleDelivery,
            ErrorCode::LimitExceeded,
            ErrorCode::Timeout,
            ErrorCode::Cancelled,
            ErrorCode::ManagementApi,
            ErrorCode::Protocol,
            ErrorCode::Internal,
        ];
        let set: HashSet<_> = all.iter().map(|c| c.rpc_code()).collect();
        assert_eq!(set.len(), all.len());
        for c in all {
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
        }
    }

    #[test]
    fn reply_code_mapping() {
        assert_eq!(
            CoreError::from_amqp_reply(406, "x").code,
            ErrorCode::PreconditionFailed
        );
        assert_eq!(
            CoreError::from_amqp_reply(403, "x").code,
            ErrorCode::AccessRefused
        );
        assert!(CoreError::from_amqp_reply(406, "x").hint.is_some());
    }

    #[test]
    fn messages_are_redacted() {
        let e = CoreError::internal("failed amqp://bob:hunter2@host/");
        assert!(!e.message.contains("hunter2"));
    }
}
