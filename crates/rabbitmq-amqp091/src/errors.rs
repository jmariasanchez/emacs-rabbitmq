//! Mapping lapin errors onto the core error model.

use lapin::ErrorKind;
use rabbitmq_core::error::{CoreError, ErrorCode};

pub fn map(e: &lapin::Error, what: &str) -> CoreError {
    match e.kind() {
        ErrorKind::ProtocolError(amqp) => {
            let mut err = CoreError::from_amqp_reply(amqp.get_id(), amqp.get_message().as_str());
            err.message = format!("{what}: {}", err.message);
            if amqp.get_id() == 403 && what == "connect" {
                err.code = ErrorCode::AuthenticationFailed;
                err.hint = Some("Check username/password, the vhost name, and that the user has access to the vhost.".into());
            }
            err
        }
        ErrorKind::IOError(io) => {
            CoreError::new(ErrorCode::ConnectionFailed, format!("{what}: {io}"))
                .retryable(what == "connect")
                .with_hint("Check host, port, TLS settings, and that the broker is running.")
        }
        ErrorKind::InvalidChannelState(state, _) => CoreError::new(
            ErrorCode::ChannelClosed,
            format!("{what}: channel is {state:?}"),
        ),
        ErrorKind::InvalidConnectionState(state) => CoreError::new(
            ErrorCode::ConnectionFailed,
            format!("{what}: connection is {state:?}"),
        ),
        ErrorKind::MissingHeartbeatError => CoreError::new(
            ErrorCode::ConnectionFailed,
            format!("{what}: missed broker heartbeats"),
        ),
        ErrorKind::AuthProviderError(m) => {
            CoreError::new(ErrorCode::AuthenticationFailed, format!("{what}: {m}"))
        }
        ErrorKind::ChannelsLimitReached => CoreError::new(
            ErrorCode::LimitExceeded,
            format!("{what}: channel limit reached"),
        ),
        other => CoreError::new(ErrorCode::Protocol, format!("{what}: {other:?}")),
    }
}

/// True when the error happened before anything was written for this
/// operation (so the broker definitely did not receive it).
pub fn is_pre_send(e: &lapin::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::InvalidChannelState(..)
            | ErrorKind::InvalidConnectionState(_)
            | ErrorKind::ChannelsLimitReached
    )
}
