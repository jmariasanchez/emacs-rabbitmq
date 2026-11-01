//! # rabbitmq-amqp091
//!
//! AMQP 0-9-1 adapter built on [lapin]. Provides a [`Session`] that owns one
//! broker connection and enforces the project's delivery-safety rules:
//!
//! * delivery tags are only ever settled through the channel that received
//!   them, and settlement after a channel/connection loss fails with
//!   `stale_delivery` instead of acknowledging the wrong message;
//! * publish outcomes come from publisher confirms, never from socket writes;
//! * consumers use manual acknowledgement and bounded buffers by default;
//! * reconnection never silently re-subscribes consumers or replays publishes.

pub mod convert;
pub mod errors;
pub mod session;
pub mod tls;

pub use session::{
    ConsumeOptions, ExchangeDeclare, PublishMode, Session, SessionInfo, SessionState,
};
