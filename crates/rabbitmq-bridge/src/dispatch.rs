//! Request dispatch: safety gate -> handler -> audit -> response.

use crate::handlers;
use crate::methods;
use crate::state::{State, MAX_INFLIGHT};
use rabbitmq_core::buffer::AuditOutcome;
use rabbitmq_core::error::{CoreError, ErrorCode};
use rabbitmq_core::safety::{Effect, Intent};
use rabbitmq_protocol::{Incoming, RequestId, Response, RpcError, CANCEL_METHOD};
use serde_json::Value;
use std::sync::Arc;

pub fn to_rpc_error(e: &CoreError) -> RpcError {
    RpcError {
        code: e.code.rpc_code(),
        message: e.message.clone(),
        data: Some(serde_json::to_value(e).unwrap_or(Value::Null)),
    }
}

/// Handle one inbound message. Requests are spawned as tasks so slow broker
/// operations never block the read loop; `$/cancelRequest` aborts them.
pub async fn dispatch(state: &Arc<State>, msg: Incoming) {
    let Some(method) = msg.method.clone() else {
        // Responses from the client (we never send requests) are ignored.
        return;
    };
    let params = msg.params.clone().unwrap_or(Value::Object(Default::default()));

    if method == CANCEL_METHOD {
        let id: Option<RequestId> = params.get("id").and_then(|v| serde_json::from_value(v.clone()).ok());
        if let Some(id) = id {
            cancel(state, id);
        }
        return;
    }

    let Some(id) = msg.id.clone() else {
        // Unknown notification: ignore per JSON-RPC.
        return;
    };

    let Some(spec) = methods::find(&method) else {
        state.outbox.respond(Response::err(Some(id), RpcError::new(RpcError::METHOD_NOT_FOUND, format!("unknown method {method}"))));
        return;
    };

    if state.inflight.lock().map(|i| i.len()).unwrap_or(0) >= MAX_INFLIGHT {
        let e = CoreError::new(ErrorCode::LimitExceeded, format!("more than {MAX_INFLIGHT} requests in flight")).retryable(true);
        state.outbox.respond(Response::err(Some(id), to_rpc_error(&e)));
        return;
    }

    let st = state.clone();
    let rid = id.clone();
    // The task waits until it is registered in `inflight`, so a fast handler
    // can never finish before registration (which would drop its response).
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = registered_rx.await;
        let result = run(&st, spec, &method, &params).await;
        // Remove from inflight before responding so a late cancel is a no-op.
        let was_inflight = st.inflight.lock().map(|mut i| i.remove(&rid).is_some()).unwrap_or(true);
        if !was_inflight {
            return; // cancelled: the cancel path already responded
        }
        st.outbox.respond(match result {
            Ok(v) => Response::ok(rid, v),
            Err(e) => Response::err(Some(rid), to_rpc_error(&e)),
        });
    });
    if let Ok(mut i) = state.inflight.lock() {
        i.insert(id, (task.abort_handle(), spec.name));
    }
    let _ = registered_tx.send(());
}

fn cancel(state: &Arc<State>, id: RequestId) {
    let entry = state.inflight.lock().ok().and_then(|mut i| i.remove(&id));
    if let Some((handle, method)) = entry {
        handle.abort();
        let effect = methods::find(method).map(|m| m.effect).unwrap_or(Effect::Observe);
        // Cancelling an intervention after it may have reached the broker is
        // ambiguous: say so rather than implying nothing happened.
        let e = CoreError::new(ErrorCode::Cancelled, format!("{method} cancelled by client"))
            .ambiguous(effect.is_intervention());
        if effect.is_intervention() {
            if let Ok(mut a) = state.audit.lock() {
                a.record("?", method, effect, None, AuditOutcome::Uncertain, Some("cancelled".into()), &Value::Null);
            }
        }
        state.outbox.respond(Response::err(Some(id), RpcError { code: RpcError::CANCELLED, message: e.message.clone(), data: serde_json::to_value(&e).ok() }));
    }
}

async fn run(state: &Arc<State>, spec: &'static methods::Method, method: &str, params: &Value) -> Result<Value, CoreError> {
    // Safety gate. Session-less bridge methods are always observations.
    let session_id = params.get("session").and_then(Value::as_str).map(String::from);
    let session = match &session_id {
        Some(id) if !method.starts_with("session.open") => state.sessions.lock().await.get(id).cloned(),
        _ => None,
    };
    let resource = session.as_ref().and_then(|s| spec.resource(params, &s.profile.vhost));
    let profile_name = session.as_ref().map(|s| s.profile.name.clone()).unwrap_or_default();

    if let Some(sess) = &session {
        let intent = Intent {
            method,
            effect: spec.effect,
            resource: resource.clone(),
            consent: params.get("consent").and_then(Value::as_bool).unwrap_or(false),
            confirm: params.get("confirm").and_then(Value::as_str),
        };
        if let Err(e) = sess.profile.safety().check(&intent) {
            if spec.effect.is_intervention() {
                if let Ok(mut a) = state.audit.lock() {
                    a.record(&profile_name, method, spec.effect, resource.clone(), AuditOutcome::Denied, Some(e.message.clone()), params);
                }
            }
            return Err(e);
        }
    }

    let r = handlers::handle(state, method, params).await;

    if spec.effect.is_intervention() {
        let outcome = match &r {
            Ok(v) => {
                // A publish where anything is uncertain is recorded as uncertain.
                let uncertain = v.pointer("/summary/uncertain").and_then(Value::as_u64).unwrap_or(0) > 0;
                if uncertain { AuditOutcome::Uncertain } else { AuditOutcome::Succeeded }
            }
            Err(e) if e.ambiguous => AuditOutcome::Uncertain,
            Err(_) => AuditOutcome::Failed,
        };
        if let Ok(mut a) = state.audit.lock() {
            let rec = a.record(&profile_name, method, spec.effect, resource, outcome, r.as_ref().err().map(|e| e.message.clone()), params);
            state.outbox.notify("$/audit", serde_json::to_value(rec).unwrap_or(Value::Null));
        }
    }
    r
}
