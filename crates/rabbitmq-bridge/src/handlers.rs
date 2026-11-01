//! Method handlers. Each receives validated params and returns JSON.
//! Safety checks have already passed by the time a handler runs.

use crate::methods::{self, effect_name, METHODS};
use crate::state::{BridgeSession, State};
use rabbitmq_amqp091::session::{ConsumeOptions, ExchangeDeclare, PublishMode};
use rabbitmq_amqp10::{Address, ReceiverOptions};
use rabbitmq_core::capabilities::{BrokerVersion, FeatureMatrix, QueueSpec, QueueType};
use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::message::*;
use rabbitmq_core::profile::Profile;
use rabbitmq_core::topology::{diff, Topology};
use rabbitmq_management::{topology_from_definitions, ListQuery, Policy, Sampled};
use rabbitmq_protocol::PROTOCOL_VERSION;
use rabbitmq_streams::{CreateStream, ReadRequest, StreamPublish, SubscribeRequest};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;

fn p<T: DeserializeOwned>(params: &Value) -> Result<T> {
    serde_json::from_value(params.clone())
        .map_err(|e| CoreError::invalid(format!("invalid params: {e}")))
}

fn s(params: &Value, key: &str) -> Result<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| CoreError::invalid(format!("missing string parameter \"{key}\"")))
}

fn opt_s(params: &Value, key: &str) -> Option<String> {
    params.get(key).and_then(Value::as_str).map(String::from)
}

fn b(params: &Value, key: &str) -> bool {
    params.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn u(params: &Value, key: &str, default: u64) -> u64 {
    params.get(key).and_then(Value::as_u64).unwrap_or(default)
}

fn obj(params: &Value, key: &str) -> Map<String, Value> {
    params
        .get(key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn ok<T: serde::Serialize>(v: T) -> Result<Value> {
    serde_json::to_value(v).map_err(|e| CoreError::internal(e.to_string()))
}

fn sampled<T: serde::Serialize>(v: T) -> Result<Value> {
    ok(Sampled::now(v))
}

fn vhost_of(sess: &BridgeSession, params: &Value) -> String {
    opt_s(params, "vhost").unwrap_or_else(|| sess.profile.vhost.clone())
}

/// AMQP 0-9-1 adapter calls require the session's vhost: a channel is bound
/// to the vhost of its connection.
fn require_session_vhost(sess: &BridgeSession, params: &Value) -> Result<()> {
    if let Some(v) = opt_s(params, "vhost") {
        if v != sess.profile.vhost {
            return Err(CoreError::invalid(format!(
                "this session is connected to vhost \"{}\"; open another session for \"{v}\" or use the mgmt.* methods",
                sess.profile.vhost
            )));
        }
    }
    Ok(())
}

pub async fn handle(state: &Arc<State>, method: &str, params: &Value) -> Result<Value> {
    match method {
        "bridge.hello" => Ok(json!({
            "name": "rabbitmq-bridge",
            "version": env!("CARGO_PKG_VERSION"),
            "protocol_version": PROTOCOL_VERSION,
            "capabilities": ["amqp091", "amqp10", "streams", "management", "routing-simulation", "audit", "cancellation"],
            "limits": {"max_inflight": crate::state::MAX_INFLIGHT, "notify_queue": crate::output::NOTIFY_QUEUE},
        })),
        "bridge.ping" => Ok(json!({"pong": true, "at": now_millis()})),
        "bridge.methods" => Ok(Value::Array(
            METHODS
                .iter()
                .map(|m| json!({"name": m.name, "effect": effect_name(m.effect), "summary": m.summary, "target": m.kind}))
                .collect(),
        )),
        "bridge.audit" => {
            let n = u(params, "limit", 100) as usize;
            ok(state.audit.lock().map(|a| a.recent(n)).unwrap_or_default())
        }
        "bridge.diagnostics" => diagnostics(state).await,
        "bridge.shutdown" => {
            state.close_all().await;
            Ok(json!({"closed": true}))
        }
        "session.open" => session_open(state, params).await,
        "session.close" => {
            let id = s(params, "session")?;
            let sess = state.sessions.lock().await.remove(&id).ok_or_else(|| CoreError::not_found(format!("no session {id}")))?;
            sess.close().await;
            Ok(json!({"closed": id}))
        }
        "session.list" => {
            let sessions: Vec<Arc<BridgeSession>> = state.sessions.lock().await.values().cloned().collect();
            let mut out = Vec::new();
            for s in sessions {
                out.push(json!({
                    "id": s.id, "profile": s.profile.name, "uri": s.profile.display_uri(),
                    "read_only": s.profile.read_only,
                    "amqp091": s.amqp_if_open().is_some(),
                    "management": s.mgmt.is_some(),
                    "version": s.features.as_ref().map(|f| format!("{}.{}.{}", f.version.0, f.version.1, f.version.2)),
                }));
            }
            Ok(Value::Array(out))
        }
        _ => {
            let sess = state.session(&s(params, "session")?).await?;
            session_method(state, &sess, method, params).await
        }
    }
}

async fn session_open(state: &Arc<State>, params: &Value) -> Result<Value> {
    let profile: Profile = p(params
        .get("profile")
        .ok_or_else(|| CoreError::invalid("missing \"profile\""))?)?;
    let id = opt_s(params, "session").unwrap_or_else(|| profile.name.clone());
    if state.sessions.lock().await.contains_key(&id) {
        return Err(CoreError::new(
            ErrorCode::PreconditionFailed,
            format!("session {id} is already open"),
        ));
    }
    let mut sess = BridgeSession::new(id.clone(), profile, state.shared_sink());
    // Probe the Management API for version-gated features (best effort).
    let mut warnings = Vec::new();
    if let Some(m) = &sess.mgmt {
        match m.overview().await {
            Ok(o) => {
                sess.features =
                    BrokerVersion::parse(&o.rabbitmq_version).map(FeatureMatrix::for_version)
            }
            Err(e) => warnings.push(format!("management API unavailable: {}", e.message)),
        }
    }
    let sess = Arc::new(sess);
    // Connect AMQP eagerly unless asked not to, so connection errors surface now.
    if !b(params, "lazy") {
        if let Err(e) = sess.amqp().await {
            if sess.mgmt.is_none() {
                return Err(e);
            }
            warnings.push(format!("AMQP 0-9-1 connection failed: {}", e.message));
        }
    }
    state.sessions.lock().await.insert(id.clone(), sess.clone());
    let mut info = sess.info().await;
    info["warnings"] = json!(warnings);
    Ok(info)
}

async fn session_method(
    state: &Arc<State>,
    sess: &Arc<BridgeSession>,
    method: &str,
    params: &Value,
) -> Result<Value> {
    let limits = sess.profile.limits;
    let timeout = Duration::from_millis(u(params, "timeout_ms", limits.timeout_ms));
    match method {
        "session.info" => Ok(sess.info().await),

        // ------------------------------------------------ AMQP 0-9-1 topology
        "exchange.declare" => {
            require_session_vhost(sess, params)?;
            let d: ExchangeDeclare = p(params)?;
            sess.amqp().await?.declare_exchange(&d).await?;
            Ok(json!({"declared": d.name}))
        }
        "exchange.delete" => {
            require_session_vhost(sess, params)?;
            let name = s(params, "name")?;
            sess.amqp()
                .await?
                .delete_exchange(&name, b(params, "if_unused"))
                .await?;
            Ok(json!({"deleted": name}))
        }
        "queue.declare" => {
            require_session_vhost(sess, params)?;
            let qt = QueueType::parse(
                params
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("classic"),
            )?;
            let spec = QueueSpec {
                name: s(params, "name").unwrap_or_default(),
                queue_type: qt,
                durable: params
                    .get("durable")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                exclusive: b(params, "exclusive"),
                auto_delete: b(params, "auto_delete"),
                arguments: obj(params, "arguments"),
            };
            ok(sess.amqp().await?.declare_queue(spec, false).await?)
        }
        "queue.inspect" => {
            require_session_vhost(sess, params)?;
            let spec = QueueSpec {
                name: s(params, "name")?,
                queue_type: QueueType::Classic,
                durable: true,
                exclusive: false,
                auto_delete: false,
                arguments: Map::new(),
            };
            ok(sess.amqp().await?.declare_queue(spec, true).await?)
        }
        "queue.delete" => {
            require_session_vhost(sess, params)?;
            let name = s(params, "name")?;
            let n = sess
                .amqp()
                .await?
                .delete_queue(&name, b(params, "if_unused"), b(params, "if_empty"))
                .await?;
            Ok(json!({"deleted": name, "messages_deleted": n}))
        }
        "queue.purge" => {
            require_session_vhost(sess, params)?;
            let name = s(params, "name")?;
            let n = sess.amqp().await?.purge_queue(&name).await?;
            Ok(json!({"purged": name, "messages_purged": n}))
        }
        "binding.create" | "binding.delete" => {
            require_session_vhost(sess, params)?;
            let dt = opt_s(params, "destination_type").unwrap_or_else(|| "queue".into());
            if dt != "queue" && dt != "exchange" {
                return Err(CoreError::invalid(
                    "destination_type must be \"queue\" or \"exchange\"",
                ));
            }
            let dest = s(params, "destination")?;
            let src = s(params, "source")?;
            let rk = opt_s(params, "routing_key").unwrap_or_default();
            sess.amqp()
                .await?
                .bind(
                    &dt,
                    &dest,
                    &src,
                    &rk,
                    &obj(params, "arguments"),
                    method == "binding.delete",
                )
                .await?;
            Ok(
                json!({"source": src, "destination": dest, "destination_type": dt, "routing_key": rk, "bound": method == "binding.create"}),
            )
        }

        // ------------------------------------------------ AMQP 0-9-1 messaging
        "message.publish" => {
            require_session_vhost(sess, params)?;
            #[derive(Deserialize)]
            struct Pub {
                messages: Vec<OutboundMessage>,
                #[serde(default)]
                mode: PublishMode,
            }
            let pb: Pub = p(params)?;
            if pb.messages.is_empty() {
                return Err(CoreError::invalid("messages must not be empty"));
            }
            let outcomes = sess
                .amqp()
                .await?
                .publish(&pb.messages, pb.mode, timeout)
                .await?;
            let summary = PublishSummary::from_outcomes(&outcomes);
            Ok(json!({"summary": summary, "outcomes": outcomes}))
        }
        "message.get" => {
            require_session_vhost(sess, params)?;
            let q = s(params, "queue")?;
            let count = u(params, "count", 1) as usize;
            ok(sess
                .amqp()
                .await?
                .get(&q, count, b(params, "auto_ack"))
                .await?)
        }
        "message.settle" => {
            let how: Settlement = p(params
                .get("outcome")
                .ok_or_else(|| CoreError::invalid("missing \"outcome\""))?)?;
            let ids: Vec<String> = match params.get("ids") {
                Some(v) => p(v)?,
                None => vec![s(params, "id")?],
            };
            let multiple = b(params, "multiple");
            let a = sess.amqp().await?;
            let mut results = Vec::new();
            let mut errors = Vec::new();
            for id in ids {
                match a.settle(&id, how, multiple).await {
                    Ok(r) => results.extend(r),
                    Err(e) => errors.push(json!({"id": id, "error": e})),
                }
            }
            Ok(json!({"settled": results, "errors": errors}))
        }
        "message.unsettled" => Ok(json!(sess.amqp().await?.unsettled_ids().await)),
        "consumer.start" => {
            require_session_vhost(sess, params)?;
            let o: ConsumeOptions = p(params)?;
            ok(sess.amqp().await?.consume(o).await?)
        }
        "consumer.cancel" => ok(sess.amqp().await?.cancel(&s(params, "consumer")?).await?),
        "consumer.pause" => ok(sess
            .amqp()
            .await?
            .set_paused(&s(params, "consumer")?, true)
            .await?),
        "consumer.resume" => ok(sess
            .amqp()
            .await?
            .set_paused(&s(params, "consumer")?, false)
            .await?),
        "consumer.drain" => {
            let (msgs, dropped) = sess
                .amqp()
                .await?
                .drain(&s(params, "consumer")?, u(params, "max", 500) as usize)
                .await?;
            Ok(json!({"messages": msgs, "dropped": dropped}))
        }

        // ------------------------------------------------ routing & topology
        "topology.snapshot" => {
            let vh = vhost_of(sess, params);
            sampled(sess.mgmt()?.topology(&vh).await?)
        }
        "topology.dot" => {
            let vh = vhost_of(sess, params);
            Ok(json!({"dot": sess.mgmt()?.topology(&vh).await?.to_dot()}))
        }
        "topology.lint" => {
            let vh = vhost_of(sess, params);
            Ok(json!({"findings": sess.mgmt()?.topology(&vh).await?.lint()}))
        }
        "routing.simulate" => {
            let topo: Topology = match params.get("topology") {
                Some(t) => p(t)?,
                None => sess.mgmt()?.topology(&vhost_of(sess, params)).await?,
            };
            let ex = opt_s(params, "exchange").unwrap_or_default();
            let rk = opt_s(params, "routing_key").unwrap_or_default();
            ok(topo.simulate(&ex, &rk, &obj(params, "headers")))
        }
        "topology.diff" => {
            let vh = vhost_of(sess, params);
            let live = sess.mgmt()?.topology(&vh).await?;
            let other: Topology = if let Some(defs) = params.get("definitions") {
                topology_from_definitions(defs, Some(&vh))
            } else if let Some(other) = opt_s(params, "other_session") {
                let o = state.session(&other).await?;
                let ovh = opt_s(params, "other_vhost").unwrap_or_else(|| o.profile.vhost.clone());
                o.mgmt()?.topology(&ovh).await?
            } else {
                return Err(CoreError::invalid(
                    "pass either \"definitions\" or \"other_session\"",
                ));
            };
            ok(diff(&live, &other))
        }

        // ------------------------------------------------ streams
        "stream.create" => {
            let c: CreateStream = p(params)?;
            sess.stream().await?.create(&c).await?;
            Ok(json!({"created": c.name, "super_stream": c.partitions.is_some()}))
        }
        "stream.delete" => {
            let n = s(params, "name")?;
            sess.stream()
                .await?
                .delete(&n, b(params, "super_stream"))
                .await?;
            Ok(json!({"deleted": n}))
        }
        "stream.publish" => {
            let r: StreamPublish = p(params)?;
            let outcomes = sess.stream().await?.publish(&r).await?;
            Ok(json!({"summary": PublishSummary::from_outcomes(&outcomes), "outcomes": outcomes}))
        }
        "stream.read" => ok(sess
            .stream()
            .await?
            .read(&p::<ReadRequest>(params)?)
            .await?),
        "stream.subscribe" => ok(sess
            .stream()
            .await?
            .subscribe(p::<SubscribeRequest>(params)?)
            .await?),
        "stream.unsubscribe" => ok(sess
            .stream()
            .await?
            .unsubscribe(&s(params, "subscription")?)
            .await?),
        "stream.pause" => ok(sess
            .stream()
            .await?
            .set_paused(&s(params, "subscription")?, true)
            .await?),
        "stream.resume" => ok(sess
            .stream()
            .await?
            .set_paused(&s(params, "subscription")?, false)
            .await?),
        "stream.subscriptions" => ok(sess.stream().await?.subscriptions().await),
        "stream.metadata" => {
            let streams: Vec<String> = match params.get("streams") {
                Some(v) => p(v)?,
                None => vec![s(params, "stream")?],
            };
            sess.stream().await?.metadata(&streams).await
        }
        "stream.partitions" => ok(sess
            .stream()
            .await?
            .partitions(&s(params, "super_stream")?)
            .await?),
        "stream.route" => ok(sess
            .stream()
            .await?
            .route(&s(params, "super_stream")?, &s(params, "routing_key")?)
            .await?),
        "stream.query_offset" => ok(sess
            .stream()
            .await?
            .query_offset(&s(params, "consumer")?, &s(params, "stream")?)
            .await?),
        "stream.store_offset" => {
            sess.stream()
                .await?
                .store_offset(
                    &s(params, "consumer")?,
                    &s(params, "stream")?,
                    u(params, "offset", 0),
                )
                .await?;
            Ok(json!({"stored": true}))
        }
        "stream.producer_sequence" => ok(sess
            .stream()
            .await?
            .producer_sequence(&s(params, "producer")?, &s(params, "stream")?)
            .await?),

        // ------------------------------------------------ AMQP 1.0
        "amqp10.sender.open" => ok(sess
            .amqp10()
            .await?
            .open_sender(&p::<Address>(
                params.get("address").unwrap_or(&Value::Null),
            )?)
            .await?),
        "amqp10.send" => {
            #[derive(Deserialize)]
            struct M {
                payload: Payload,
                #[serde(default)]
                properties: MessageProperties,
            }
            let msgs: Vec<M> = p(params.get("messages").unwrap_or(&Value::Null))?;
            let msgs: Vec<_> = msgs
                .into_iter()
                .map(|m| (m.payload, m.properties))
                .collect();
            let outcomes = sess
                .amqp10()
                .await?
                .send(&s(params, "link")?, &msgs, timeout)
                .await?;
            Ok(json!({"summary": PublishSummary::from_outcomes(&outcomes), "outcomes": outcomes}))
        }
        "amqp10.sender.close" => {
            sess.amqp10()
                .await?
                .close_sender(&s(params, "link")?)
                .await?;
            Ok(json!({"closed": true}))
        }
        "amqp10.receiver.open" => ok(sess
            .amqp10()
            .await?
            .open_receiver(p::<ReceiverOptions>(params)?)
            .await?),
        "amqp10.settle" => {
            let how: Settlement = p(params
                .get("outcome")
                .ok_or_else(|| CoreError::invalid("missing \"outcome\""))?)?;
            sess.amqp10()
                .await?
                .settle(&s(params, "link")?, u(params, "delivery_id", 0) as u32, how)
                .await?;
            Ok(json!({"settled": true}))
        }
        "amqp10.credit" => {
            sess.amqp10()
                .await?
                .set_credit(&s(params, "link")?, u(params, "credit", 10) as u32)
                .await?;
            Ok(json!({"credit": u(params, "credit", 10)}))
        }
        "amqp10.drain" => {
            sess.amqp10().await?.drain(&s(params, "link")?).await?;
            Ok(json!({"drained": true}))
        }
        "amqp10.receiver.close" => {
            sess.amqp10()
                .await?
                .close_receiver(&s(params, "link")?)
                .await?;
            Ok(json!({"closed": true}))
        }
        "amqp10.links" => ok(sess.amqp10().await?.links().await),

        m if m.starts_with("mgmt.") => mgmt(sess, m, params).await,
        other => Err(CoreError::new(
            ErrorCode::InvalidParams,
            format!("method {other} not implemented"),
        )),
    }
}

async fn mgmt(sess: &Arc<BridgeSession>, method: &str, params: &Value) -> Result<Value> {
    let m = sess.mgmt()?;
    let vh = opt_s(params, "vhost");
    let vh_or = || vh.clone().unwrap_or_else(|| sess.profile.vhost.clone());
    let lq = || -> Result<ListQuery> { p::<ListQuery>(params) };
    match method {
        "mgmt.overview" => sampled(m.overview().await?),
        "mgmt.nodes" => sampled(m.nodes().await?),
        "mgmt.node" => sampled(m.node(&s(params, "name")?).await?),
        "mgmt.health" => sampled(m.health().await?),
        "mgmt.feature_flags" => ok(m.feature_flags().await?),
        "mgmt.deprecated_features" => ok(m.deprecated_features_used().await?),
        "mgmt.whoami" => ok(m.whoami().await?),
        "mgmt.vhosts" => sampled(m.vhosts().await?),
        "mgmt.queues" => sampled(m.queues(vh.as_deref(), &lq()?).await?),
        "mgmt.queue" => sampled(m.queue(&vh_or(), &s(params, "name")?).await?),
        "mgmt.queue_bindings" => ok(m.queue_bindings(&vh_or(), &s(params, "name")?).await?),
        "mgmt.exchanges" => sampled(m.exchanges(vh.as_deref(), &lq()?).await?),
        "mgmt.exchange" => sampled(m.exchange(&vh_or(), &s(params, "name")?).await?),
        "mgmt.bindings" => ok(m.bindings(vh.as_deref()).await?),
        "mgmt.connections" => sampled(m.connections(&lq()?).await?),
        "mgmt.connection" => sampled(m.connection(&s(params, "name")?).await?),
        "mgmt.channels" => sampled(m.channels(&lq()?).await?),
        "mgmt.consumers" => sampled(m.consumers(vh.as_deref()).await?),
        "mgmt.users" => ok(m.users().await?),
        "mgmt.permissions" => ok(m.permissions().await?),
        "mgmt.user_permissions" => ok(m.user_permissions(&s(params, "user")?).await?),
        "mgmt.topic_permissions" => ok(m.topic_permissions().await?),
        "mgmt.policies" => ok(m.policies(vh.as_deref()).await?),
        "mgmt.operator_policies" => ok(m.operator_policies(vh.as_deref()).await?),
        "mgmt.parameters" => ok(m.parameters(opt_s(params, "component").as_deref()).await?),
        "mgmt.global_parameters" => ok(m.global_parameters().await?),
        "mgmt.vhost_limits" => ok(m.vhost_limits(vh.as_deref()).await?),
        "mgmt.user_limits" => ok(m.user_limits().await?),
        "mgmt.definitions.export" => Ok(rabbitmq_core::redact::redact_json(
            &m.export_definitions(vh.as_deref()).await?,
        )),
        "mgmt.stream_connections" => ok(m.stream_connections().await?),
        "mgmt.stream_publishers" => ok(m.stream_publishers(vh.as_deref()).await?),
        "mgmt.stream_consumers" => ok(m.stream_consumers(vh.as_deref()).await?),
        "mgmt.dead_letter_report" => dead_letter_report(m, &vh_or()).await,

        "mgmt.vhost.create" => {
            let name = s(params, "name")?;
            let mut body = json!({});
            for k in ["description", "tags", "default_queue_type", "tracing"] {
                if let Some(v) = params.get(k) {
                    body[k] = v.clone();
                }
            }
            m.create_vhost(&name, &body).await?;
            Ok(json!({"created": name}))
        }
        "mgmt.vhost.delete" => {
            let n = s(params, "name")?;
            m.delete_vhost(&n).await?;
            Ok(json!({"deleted": n}))
        }
        "mgmt.user.put" => {
            let n = s(params, "name")?;
            m.put_user(
                &n,
                opt_s(params, "password").as_deref(),
                &opt_s(params, "tags").unwrap_or_default(),
            )
            .await?;
            Ok(json!({"user": n}))
        }
        "mgmt.user.delete" => {
            let n = s(params, "name")?;
            m.delete_user(&n).await?;
            Ok(json!({"deleted": n}))
        }
        "mgmt.permissions.set" => {
            let user = s(params, "user")?;
            m.set_permissions(
                &vh_or(),
                &user,
                &s(params, "configure")?,
                &s(params, "write")?,
                &s(params, "read")?,
            )
            .await?;
            Ok(json!({"user": user, "vhost": vh_or()}))
        }
        "mgmt.permissions.clear" => {
            let user = s(params, "user")?;
            m.clear_permissions(&vh_or(), &user).await?;
            Ok(json!({"cleared": user, "vhost": vh_or()}))
        }
        "mgmt.topic_permissions.set" => {
            let user = s(params, "user")?;
            m.set_topic_permissions(
                &vh_or(),
                &user,
                &s(params, "exchange")?,
                &s(params, "write")?,
                &s(params, "read")?,
            )
            .await?;
            Ok(json!({"user": user}))
        }
        "mgmt.topic_permissions.clear" => {
            let user = s(params, "user")?;
            m.clear_topic_permissions(&vh_or(), &user).await?;
            Ok(json!({"cleared": user}))
        }
        "mgmt.policy.put" | "mgmt.operator_policy.put" => {
            let mut pv = params.clone();
            pv["vhost"] = json!(vh_or());
            if pv.get("apply-to").is_none() {
                if let Some(a) = pv.get("apply_to").cloned() {
                    pv["apply-to"] = a;
                }
            }
            let pol: Policy = p(&pv)?;
            if method == "mgmt.policy.put" {
                m.put_policy(&pol).await?;
            } else {
                m.put_operator_policy(&pol).await?;
            }
            Ok(json!({"policy": pol.name}))
        }
        "mgmt.policy.delete" => {
            let n = s(params, "name")?;
            m.delete_policy(&vh_or(), &n).await?;
            Ok(json!({"deleted": n}))
        }
        "mgmt.operator_policy.delete" => {
            let n = s(params, "name")?;
            m.delete_operator_policy(&vh_or(), &n).await?;
            Ok(json!({"deleted": n}))
        }
        "mgmt.vhost_limit.set" => {
            let l = s(params, "limit")?;
            let v = params
                .get("value")
                .and_then(Value::as_i64)
                .ok_or_else(|| CoreError::invalid("missing integer \"value\""))?;
            m.set_vhost_limit(&vh_or(), &l, v).await?;
            Ok(json!({"limit": l, "value": v}))
        }
        "mgmt.vhost_limit.clear" => {
            let l = s(params, "limit")?;
            m.clear_vhost_limit(&vh_or(), &l).await?;
            Ok(json!({"cleared": l}))
        }
        "mgmt.connection.close" => {
            let n = s(params, "name")?;
            m.close_connection(
                &n,
                &opt_s(params, "reason").unwrap_or_else(|| "closed via rabbitmq.el".into()),
            )
            .await?;
            Ok(json!({"closed": n}))
        }
        "mgmt.definitions.import" => {
            let defs = params
                .get("definitions")
                .ok_or_else(|| CoreError::invalid("missing \"definitions\""))?;
            m.import_definitions(vh.as_deref(), defs).await?;
            Ok(json!({"imported": true}))
        }
        other => Err(CoreError::new(
            ErrorCode::InvalidParams,
            format!("method {other} not implemented"),
        )),
    }
}

/// Queues with dead-lettering configured (by argument or effective policy),
/// where messages go, and whether the target exists.
async fn dead_letter_report(
    m: &rabbitmq_management::ManagementClient,
    vhost: &str,
) -> Result<Value> {
    let queues: Vec<rabbitmq_management::QueueInfo> =
        serde_json::from_value(m.queues(Some(vhost), &ListQuery::default()).await?)
            .map_err(|e| CoreError::internal(e.to_string()))?;
    let topo = m.topology(vhost).await?;
    let mut out = Vec::new();
    for q in &queues {
        let pol = q.effective_policy_definition.clone().unwrap_or(Value::Null);
        let dlx = q
            .arguments
            .get("x-dead-letter-exchange")
            .or_else(|| pol.get("dead-letter-exchange"))
            .and_then(Value::as_str);
        let Some(dlx) = dlx else { continue };
        let dlrk = q
            .arguments
            .get("x-dead-letter-routing-key")
            .or_else(|| pol.get("dead-letter-routing-key"))
            .and_then(Value::as_str);
        let strategy = q
            .arguments
            .get("x-dead-letter-strategy")
            .or_else(|| pol.get("dead-letter-strategy"))
            .and_then(Value::as_str)
            .unwrap_or("at-most-once");
        let exists = topo.exchange(dlx).is_some() || dlx.is_empty();
        let pred = topo.simulate(dlx, dlrk.unwrap_or(&q.name), &Map::new());
        out.push(json!({
            "queue": q.name,
            "type": q.queue_type(),
            "dead_letter_exchange": dlx,
            "dead_letter_routing_key": dlrk,
            "strategy": strategy,
            "delivery_limit": q.arguments.get("x-delivery-limit").or_else(|| pol.get("delivery-limit")),
            "exchange_exists": exists,
            "predicted_targets": pred.queues,
            "dead_letters_dropped": pred.unroutable,
            "messages": q.messages,
        }));
    }
    sampled(
        json!({"vhost": vhost, "queues": out, "note": "Targets are predictions from current bindings."}),
    )
}

async fn diagnostics(state: &Arc<State>) -> Result<Value> {
    let sessions: Vec<Arc<BridgeSession>> = state.sessions.lock().await.values().cloned().collect();
    let mut infos = Vec::new();
    for s in sessions {
        infos.push(s.info().await);
    }
    let recent: Vec<Value> = state
        .sink
        .recent
        .lock()
        .map(|r| r.iter().cloned().collect())
        .unwrap_or_default();
    let audit = state.audit.lock().map(|a| a.recent(50)).unwrap_or_default();
    Ok(rabbitmq_core::redact::redact_json(&json!({
        "bridge": {
            "version": env!("CARGO_PKG_VERSION"),
            "protocol_version": PROTOCOL_VERSION,
            "started_at": state.started_at,
            "now": now_millis(),
            "inflight": state.inflight.lock().map(|i| i.len()).unwrap_or(0),
            "notifications_dropped": state.outbox.dropped(),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "sessions": infos,
        "recent_events": recent,
        "recent_audit": audit,
        "methods": methods::METHODS.len(),
    })))
}
