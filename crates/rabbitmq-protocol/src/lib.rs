//! # rabbitmq-protocol
//!
//! The versioned contract between `rabbitmq.el` and `rabbitmq-bridge`.
//!
//! * Transport: JSON-RPC 2.0 over stdio with LSP-style `Content-Length`
//!   framing (compatible with Emacs `jsonrpc.el`).
//! * Requests carry an integer or string `id`; responses echo it.
//! * The bridge pushes notifications (`$/event`, `$/delivery`, ...) without
//!   an `id`.
//! * Cancellation: the client sends the `$/cancelRequest` notification with
//!   `{"id": <request id>}`. The bridge aborts the task and replies to the
//!   original request with error code `-32800` (`cancelled`). An operation
//!   that already reached the broker is reported as `ambiguous`.
//!
//! See `docs/protocol/RPC.md` for the complete method catalogue.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Bumped on any breaking change to method names, parameters, or result shapes.
pub const PROTOCOL_VERSION: u32 = 1;

/// Hard upper bound on a single frame to protect the bridge from a runaway peer.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_HEADER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Num(i64),
    Str(String),
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestId::Num(n) => write!(f, "{n}"),
            RequestId::Str(s) => f.write_str(s),
        }
    }
}

/// Any inbound message from the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Incoming {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    #[serde(default)]
    pub id: Option<RequestId>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL: i64 = -32603;
    pub const CANCELLED: i64 = -32800;

    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Response {
    pub jsonrpc: &'static str,
    pub id: Option<RequestId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn ok(id: RequestId, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }
    pub fn err(id: Option<RequestId>, error: RpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Notification {
    pub jsonrpc: &'static str,
    pub method: String,
    pub params: Value,
}

impl Notification {
    pub fn new(method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            method: method.into(),
            params,
        }
    }
}

/// Notification method names pushed by the bridge.
pub mod notify {
    /// Connection/session lifecycle: `{session, state, detail}`.
    pub const SESSION: &str = "$/session";
    /// Batched consumer deliveries: `{session, consumer, messages: [...], dropped}`.
    pub const DELIVERIES: &str = "$/deliveries";
    /// Consumer lifecycle: `{session, consumer, state, reason}`.
    pub const CONSUMER: &str = "$/consumer";
    /// Returned (unroutable mandatory) messages: `{session, message}`.
    pub const RETURNED: &str = "$/returned";
    /// Generic event log entry: `{level, source, message, at}`.
    pub const EVENT: &str = "$/event";
    /// Progress for long-running operations: `{id, done, total}`.
    pub const PROGRESS: &str = "$/progress";
}

/// Client-to-bridge notification for cancelling an in-flight request.
pub const CANCEL_METHOD: &str = "$/cancelRequest";

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed header: {0}")]
    Header(String),
    #[error("frame of {0} bytes exceeds limit")]
    TooLarge(usize),
    #[error("eof")]
    Eof,
}

/// Read one `Content-Length` framed message body.
pub async fn read_frame<R: AsyncBufRead + Unpin>(r: &mut R) -> Result<Vec<u8>, FrameError> {
    let mut len: Option<usize> = None;
    let mut line = String::new();
    let mut header_bytes = 0usize;
    loop {
        line.clear();
        // Limit the read itself, not just the resulting allocation.
        let n = r
            .take((MAX_HEADER_BYTES - header_bytes + 1) as u64)
            .read_line(&mut line)
            .await?;
        header_bytes += n;
        if header_bytes > MAX_HEADER_BYTES {
            return Err(FrameError::Header("header exceeds limit".into()));
        }
        if n == 0 {
            return Err(FrameError::Eof);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            if len.is_some() {
                break;
            }
            continue; // tolerate stray blank lines between frames
        }
        let (k, v) = trimmed
            .split_once(':')
            .ok_or_else(|| FrameError::Header(trimmed.to_string()))?;
        if k.trim().eq_ignore_ascii_case("content-length") {
            if len.is_some() {
                return Err(FrameError::Header("duplicate Content-Length".into()));
            }
            let n: usize = v
                .trim()
                .parse()
                .map_err(|_| FrameError::Header(trimmed.to_string()))?;
            if n > MAX_FRAME_BYTES {
                return Err(FrameError::TooLarge(n));
            }
            len = Some(n);
        }
    }
    let n = len.unwrap_or(0);
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write one framed message.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, body: &[u8]) -> std::io::Result<()> {
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    w.write_all(header.as_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

/// Describes one RPC method for discovery (`rpc.discover`) and docs.
#[derive(Debug, Clone, Serialize)]
pub struct MethodInfo {
    pub name: &'static str,
    pub effect: &'static str,
    pub summary: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn frame_roundtrip() {
        let mut out = Vec::new();
        write_frame(&mut out, br#"{"a":1}"#).await.unwrap();
        write_frame(&mut out, "é".as_bytes()).await.unwrap();
        let mut r = BufReader::new(&out[..]);
        assert_eq!(read_frame(&mut r).await.unwrap(), br#"{"a":1}"#);
        assert_eq!(read_frame(&mut r).await.unwrap(), "é".as_bytes());
        assert!(matches!(read_frame(&mut r).await, Err(FrameError::Eof)));
    }

    #[tokio::test]
    async fn header_variants() {
        let data = b"content-length: 2\r\nContent-Type: x\r\n\r\n{}";
        let mut r = BufReader::new(&data[..]);
        assert_eq!(read_frame(&mut r).await.unwrap(), b"{}");
        let bad = b"Content-Length: nope\r\n\r\n";
        let mut r = BufReader::new(&bad[..]);
        assert!(matches!(
            read_frame(&mut r).await,
            Err(FrameError::Header(_))
        ));
        let big = format!("Content-Length: {}\r\n\r\n", MAX_FRAME_BYTES + 1);
        let mut r = BufReader::new(big.as_bytes());
        assert!(matches!(
            read_frame(&mut r).await,
            Err(FrameError::TooLarge(_))
        ));
    }

    #[tokio::test]
    async fn rejects_unbounded_and_duplicate_headers() {
        for data in [
            vec![b'x'; MAX_HEADER_BYTES + 1],
            b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
            vec![b'\n'; MAX_HEADER_BYTES + 1],
        ] {
            let mut r = BufReader::new(data.as_slice());
            assert!(matches!(
                read_frame(&mut r).await,
                Err(FrameError::Header(_))
            ));
        }
    }

    #[test]
    fn message_shapes() {
        let req: Incoming =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":3,"method":"ping","params":{}}"#)
                .unwrap();
        assert_eq!(req.id, Some(RequestId::Num(3)));
        let r = serde_json::to_value(Response::ok(
            RequestId::Str("a".into()),
            serde_json::json!(1),
        ))
        .unwrap();
        assert_eq!(r, serde_json::json!({"jsonrpc":"2.0","id":"a","result":1}));
        let e = serde_json::to_value(Response::err(None, RpcError::new(-1, "x"))).unwrap();
        assert_eq!(e["id"], Value::Null);
    }
}
