//! # rabbitmq-bridge
//!
//! JSON-RPC 2.0 server over stdio for `rabbitmq.el`. See `docs/protocol/RPC.md`.
//!
//! Usage: `rabbitmq-bridge [--audit-log PATH] [--log-file PATH] [--version]`
//!
//! Logs never go to stdout (that is the protocol channel). By default they
//! go to stderr, which Emacs captures in a separate buffer.

mod dispatch;
mod handlers;
mod methods;
mod output;
mod state;

use rabbitmq_core::buffer::AuditLog;
use rabbitmq_protocol::{read_frame, FrameError, Incoming, Response, RpcError};
use tokio::io::BufReader;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!(
            "rabbitmq-bridge {} (protocol {})",
            env!("CARGO_PKG_VERSION"),
            rabbitmq_protocol::PROTOCOL_VERSION
        );
        return;
    }
    if args.iter().any(|a| a == "--methods") {
        use std::io::Write;
        let mut out = std::io::stdout().lock();
        for m in methods::METHODS {
            // Stop quietly if the reader goes away (e.g. `| head`).
            if writeln!(
                out,
                "{:<34} {:<12} {}",
                m.name,
                methods::effect_name(m.effect),
                m.summary
            )
            .is_err()
            {
                break;
            }
        }
        return;
    }
    let arg = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    let filter = tracing_subscriber::EnvFilter::try_from_env("RABBITMQ_BRIDGE_LOG")
        .unwrap_or_else(|_| "warn".into());
    match arg("--log-file").and_then(|p| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .ok()
    }) {
        Some(f) => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::sync::Mutex::new(f))
            .with_ansi(false)
            .init(),
        None => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init(),
    }

    let mut audit = AuditLog::new(1000);
    if let Some(path) = arg("--audit-log") {
        match AuditLog::new(1000).with_file(std::path::Path::new(&path)) {
            Ok(a) => audit = a,
            Err(e) => eprintln!("cannot open audit log {path}: {e}"),
        }
    }

    let (outbox, writer) = output::spawn_writer(tokio::io::stdout());
    let state = state::State::new(outbox.clone(), audit);
    let mut stdin = BufReader::new(tokio::io::stdin());

    loop {
        let frame = match read_frame(&mut stdin).await {
            Ok(f) => f,
            Err(FrameError::Eof) => break,
            Err(FrameError::Io(e)) => {
                tracing::error!("stdin: {e}");
                break;
            }
            Err(e) => {
                outbox.respond(Response::err(
                    None,
                    RpcError::new(RpcError::PARSE_ERROR, e.to_string()),
                ));
                // The body boundary is unknown after a framing error.
                break;
            }
        };
        let msg: Incoming = match serde_json::from_slice(&frame) {
            Ok(m) => m,
            Err(e) => {
                outbox.respond(Response::err(
                    None,
                    RpcError::new(RpcError::PARSE_ERROR, format!("invalid JSON: {e}")),
                ));
                continue;
            }
        };
        let shutdown = msg.method.as_deref() == Some("bridge.shutdown");
        dispatch::dispatch(&state, msg).await;
        if shutdown {
            // Let the shutdown handler close sessions and respond.
            for _ in 0..100 {
                if state.inflight.lock().map(|i| i.is_empty()).unwrap_or(true) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            break;
        }
    }

    // stdin closed (Emacs exited or killed the process) or shutdown requested:
    // release every broker resource before exiting.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), state.close_all()).await;
    drop(state);
    drop(outbox);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), writer).await;
}
