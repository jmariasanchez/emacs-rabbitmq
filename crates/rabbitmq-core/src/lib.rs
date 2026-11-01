//! # rabbitmq-core
//!
//! Protocol-independent building blocks shared by every rabbitmq-* crate:
//!
//! * [`error`]: structured errors with stable codes and retry/ambiguity flags.
//! * [`profile`]: connection profiles, secrets, and resource limits.
//! * [`safety`]: the observation-versus-intervention policy.
//! * [`message`]: protocol-neutral messages, payload encodings, publish outcomes.
//! * [`topology`]: topology model, routing prediction, linting, and diffing.
//! * [`capabilities`]: queue-type and broker-version gating.
//! * [`buffer`]: bounded buffers and the client audit log.
//! * [`redact`]: credential redaction for logs and reports.
//!
//! This crate has no dependency on Emacs or on any broker client library.

pub mod buffer;
pub mod capabilities;
pub mod error;
pub mod events;
pub mod message;
pub mod profile;
pub mod redact;
pub mod safety;
pub mod topology;

pub use error::{CoreError, ErrorCode, Result};
