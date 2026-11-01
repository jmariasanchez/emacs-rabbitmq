//! # rabbitmq-management
//!
//! Typed client for the RabbitMQ Management HTTP API (`/api/...`).
//!
//! Resources are modelled as typed structs carrying the fields this project
//! relies on, plus an `extra` map (via `#[serde(flatten)]`) that preserves
//! every other field the broker returns. That keeps the client forward
//! compatible with new broker fields while giving callers compile-time
//! checked access to the important ones.
//!
//! Statistics from the Management API are sampled and eventually consistent.
//! Every metric-bearing response is wrapped in [`Sampled`] so the UI can show
//! when it was fetched and mark it stale.

use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::profile::{pct, Profile};
use rabbitmq_core::topology::{Binding, Exchange, Queue, Topology};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::time::Duration;

pub type Extra = Map<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Overview {
    #[serde(default)]
    pub rabbitmq_version: String,
    #[serde(default)]
    pub erlang_version: String,
    #[serde(default)]
    pub product_name: Option<String>,
    #[serde(default)]
    pub product_version: Option<String>,
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub node: String,
    #[serde(default)]
    pub object_totals: Value,
    #[serde(default)]
    pub queue_totals: Value,
    #[serde(default)]
    pub message_stats: Value,
    #[serde(default)]
    pub listeners: Vec<Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    #[serde(default)]
    pub running: bool,
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub mem_used: Option<u64>,
    #[serde(default)]
    pub mem_limit: Option<u64>,
    #[serde(default)]
    pub mem_alarm: Option<bool>,
    #[serde(default)]
    pub disk_free: Option<u64>,
    #[serde(default)]
    pub disk_free_limit: Option<u64>,
    #[serde(default)]
    pub disk_free_alarm: Option<bool>,
    #[serde(default)]
    pub fd_used: Option<u64>,
    #[serde(default)]
    pub fd_total: Option<u64>,
    #[serde(default)]
    pub sockets_used: Option<u64>,
    #[serde(default)]
    pub sockets_total: Option<u64>,
    #[serde(default)]
    pub proc_used: Option<u64>,
    #[serde(default)]
    pub proc_total: Option<u64>,
    #[serde(default)]
    pub uptime: Option<u64>,
    #[serde(default)]
    pub partitions: Vec<Value>,
    #[serde(default)]
    pub being_drained: Option<bool>,
    #[serde(default)]
    pub rabbitmq_version: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    pub vhost: String,
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub exclusive: Option<bool>,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub effective_policy_definition: Option<Value>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub node: Option<String>,
    /// Quorum/stream leader.
    #[serde(default)]
    pub leader: Option<String>,
    #[serde(default)]
    pub members: Option<Vec<String>>,
    #[serde(default)]
    pub online: Option<Vec<String>>,
    #[serde(default)]
    pub messages: Option<u64>,
    #[serde(default)]
    pub messages_ready: Option<u64>,
    #[serde(default)]
    pub messages_unacknowledged: Option<u64>,
    #[serde(default)]
    pub consumers: Option<u64>,
    #[serde(default)]
    pub consumer_capacity: Option<f64>,
    #[serde(default)]
    pub single_active_consumer_tag: Option<String>,
    #[serde(default)]
    pub message_stats: Option<Value>,
    #[serde(default)]
    pub memory: Option<u64>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl QueueInfo {
    pub fn queue_type(&self) -> &str {
        self.kind.as_deref().unwrap_or("classic")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub name: String,
    pub vhost: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub message_stats: Option<Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingInfo {
    pub source: String,
    pub vhost: String,
    pub destination: String,
    pub destination_type: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default)]
    pub properties_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub name: String,
    #[serde(default)]
    pub vhost: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub peer_host: Option<String>,
    #[serde(default)]
    pub peer_port: Option<u16>,
    #[serde(default)]
    pub channels: Option<u64>,
    #[serde(default)]
    pub ssl: Option<bool>,
    #[serde(default)]
    pub client_properties: Option<Value>,
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub user_provided_name: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub name: String,
    #[serde(default)]
    pub number: Option<u64>,
    #[serde(default)]
    pub vhost: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub prefetch_count: Option<u64>,
    #[serde(default)]
    pub messages_unacknowledged: Option<u64>,
    #[serde(default)]
    pub messages_unconfirmed: Option<u64>,
    #[serde(default)]
    pub consumer_count: Option<u64>,
    #[serde(default)]
    pub confirm: Option<bool>,
    #[serde(default)]
    pub transactional: Option<bool>,
    #[serde(default)]
    pub connection_details: Option<Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerInfo {
    #[serde(default)]
    pub consumer_tag: String,
    #[serde(default)]
    pub queue: Value,
    #[serde(default)]
    pub ack_required: Option<bool>,
    #[serde(default)]
    pub prefetch_count: Option<u64>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub activity_status: Option<String>,
    #[serde(default)]
    pub exclusive: Option<bool>,
    #[serde(default)]
    pub channel_details: Option<Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VhostInfo {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Option<Value>,
    #[serde(default)]
    pub default_queue_type: Option<String>,
    #[serde(default)]
    pub tracing: Option<bool>,
    #[serde(default)]
    pub messages: Option<u64>,
    #[serde(default)]
    pub cluster_state: Option<Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInfo {
    pub name: String,
    #[serde(default)]
    pub tags: Value,
    /// Never returned to Emacs in clear: redacted by the bridge.
    #[serde(default, skip_serializing)]
    pub password_hash: Option<String>,
    #[serde(default)]
    pub hashing_algorithm: Option<String>,
    #[serde(default)]
    pub limits: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Permission {
    pub user: String,
    pub vhost: String,
    pub configure: String,
    pub write: String,
    pub read: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicPermission {
    pub user: String,
    pub vhost: String,
    pub exchange: String,
    pub write: String,
    pub read: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    pub vhost: String,
    pub pattern: String,
    #[serde(rename = "apply-to", default = "all")]
    pub apply_to: String,
    pub definition: Map<String, Value>,
    #[serde(default)]
    pub priority: i64,
}

fn all() -> String {
    "all".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureFlag {
    pub name: String,
    #[serde(default)]
    pub desc: Option<String>,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub stability: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Paged list response from `?page=N&page_size=M`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub page: u64,
    pub page_count: u64,
    pub page_size: u64,
    pub total_count: u64,
    #[serde(default)]
    pub filtered_count: u64,
    #[serde(default)]
    pub item_count: u64,
}

/// A value plus the time it was sampled. Management statistics are not
/// real-time; the UI uses `fetched_at` to mark stale data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sampled<T> {
    pub fetched_at: u64,
    pub source: &'static str,
    pub data: T,
}

impl<T> Sampled<T> {
    pub fn now(data: T) -> Self {
        Self {
            fetched_at: rabbitmq_core::message::now_millis(),
            source: "management_api",
            data,
        }
    }
}

/// Query options for list endpoints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub page: Option<u64>,
    #[serde(default)]
    pub page_size: Option<u64>,
    /// Name filter (regex when `use_regex`).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub use_regex: bool,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub sort_reverse: bool,
    /// Restrict returned columns (`columns=name,messages`).
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Disable per-object stats for faster listing of large topologies.
    #[serde(default)]
    pub disable_stats: bool,
}

impl ListQuery {
    fn params(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if let Some(p) = self.page {
            q.push(("page".into(), p.to_string()));
            q.push((
                "page_size".into(),
                self.page_size.unwrap_or(100).to_string(),
            ));
        }
        if let Some(n) = &self.name {
            q.push(("name".into(), n.clone()));
            if self.use_regex {
                q.push(("use_regex".into(), "true".into()));
            }
        }
        if let Some(s) = &self.sort {
            q.push(("sort".into(), s.clone()));
            if self.sort_reverse {
                q.push(("sort_reverse".into(), "true".into()));
            }
        }
        if let Some(c) = &self.columns {
            q.push(("columns".into(), c.join(",")));
        }
        if self.disable_stats {
            q.push(("disable_stats".into(), "true".into()));
            q.push(("enable_queue_totals".into(), "true".into()));
        }
        q
    }
}

#[derive(Clone)]
pub struct ManagementClient {
    base: String,
    user: String,
    password: rabbitmq_core::profile::Secret,
    http: reqwest::Client,
}

impl std::fmt::Debug for ManagementClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagementClient")
            .field("base", &self.base)
            .field("user", &self.user)
            .finish()
    }
}

fn v(vhost: &str) -> String {
    pct(vhost)
}

impl ManagementClient {
    pub fn from_profile(p: &Profile) -> Result<Self> {
        let base = p.management_url.clone().ok_or_else(|| {
            CoreError::unsupported(
                "profile has no management_url; Management API features are unavailable",
            )
            .with_hint("Set :management-url in the profile, e.g. http://localhost:15672")
        })?;
        let mut b = reqwest::Client::builder()
            .timeout(Duration::from_millis(p.limits.timeout_ms))
            .user_agent(concat!("rabbitmq.el/", env!("CARGO_PKG_VERSION")));
        if let Some(ca) = &p.tls.ca_file {
            let pem = std::fs::read(ca)
                .map_err(|e| CoreError::invalid(format!("cannot read CA file {ca}: {e}")))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| CoreError::invalid(format!("invalid CA file {ca}: {e}")))?;
            for c in certs {
                b = b.add_root_certificate(c);
            }
        }
        if let (Some(cert), Some(key)) = (&p.tls.client_cert_file, &p.tls.client_key_file) {
            let mut pem = std::fs::read(cert)
                .map_err(|e| CoreError::invalid(format!("cannot read client cert {cert}: {e}")))?;
            pem.extend(
                std::fs::read(key).map_err(|e| {
                    CoreError::invalid(format!("cannot read client key {key}: {e}"))
                })?,
            );
            let id = reqwest::Identity::from_pem(&pem)
                .map_err(|e| CoreError::invalid(format!("invalid client certificate/key: {e}")))?;
            b = b.identity(id);
        }
        if !p.tls.verify {
            b = b.danger_accept_invalid_certs(true);
        }
        let http = b
            .build()
            .map_err(|e| CoreError::internal(format!("http client: {e}")))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            user: p.username.clone(),
            password: p.password.clone(),
            http,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/{}", self.base, path.trim_start_matches('/'))
    }

    async fn send(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
        mutating: bool,
    ) -> Result<reqwest::Response> {
        let resp = req
            .basic_auth(&self.user, Some(self.password.expose()))
            .send()
            .await
            .map_err(|e| {
                let code = if e.is_timeout() {
                    ErrorCode::Timeout
                } else {
                    ErrorCode::ConnectionFailed
                };
                // A mutating request that timed out may still have been applied.
                CoreError::new(code, format!("{what}: {e}"))
                    .retryable(!mutating)
                    .ambiguous(mutating && e.is_timeout())
            })?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let reason = body["reason"]
            .as_str()
            .or_else(|| body["error"].as_str())
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("error"))
            .to_string();
        let code = match status.as_u16() {
            401 => ErrorCode::AuthenticationFailed,
            403 => ErrorCode::AccessRefused,
            404 => ErrorCode::NotFound,
            400 if reason.contains("inequivalent") => ErrorCode::PreconditionFailed,
            400 => ErrorCode::InvalidParams,
            503 => ErrorCode::ConnectionFailed,
            _ => ErrorCode::ManagementApi,
        };
        let mut e = CoreError::new(code, format!("{what}: {reason}"))
            .with_detail(json!({ "http_status": status.as_u16() }));
        if code == ErrorCode::AccessRefused {
            e = e.with_hint("The Management API user needs a management, monitoring, policymaker, or administrator tag for this operation.");
        }
        Err(e)
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.get_query(path, &[]).await
    }

    pub async fn get_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> Result<T> {
        let resp = self
            .send(
                self.http.get(self.url(path)).query(query),
                &format!("GET {path}"),
                false,
            )
            .await?;
        resp.json::<T>().await.map_err(|e| {
            CoreError::new(
                ErrorCode::ManagementApi,
                format!("GET {path}: unexpected response: {e}"),
            )
        })
    }

    pub async fn put(&self, path: &str, body: &Value) -> Result<()> {
        self.send(
            self.http.put(self.url(path)).json(body),
            &format!("PUT {path}"),
            true,
        )
        .await
        .map(|_| ())
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        let r = self
            .send(
                self.http.post(self.url(path)).json(body),
                &format!("POST {path}"),
                true,
            )
            .await?;
        let text = r.text().await.unwrap_or_default();
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    pub async fn delete(&self, path: &str, query: &[(String, String)]) -> Result<()> {
        self.send(
            self.http.delete(self.url(path)).query(query),
            &format!("DELETE {path}"),
            true,
        )
        .await
        .map(|_| ())
    }

    /// List endpoint that returns either a plain array or a page.
    async fn list<T: DeserializeOwned>(&self, path: &str, q: &ListQuery) -> Result<Value> {
        let raw: Value = self.get_query(path, &q.params()).await?;
        // Validate shape early so callers get a typed error, but return the raw
        // JSON so pages and plain arrays share one RPC shape. A `columns`
        // projection deliberately omits fields, so it is not validated.
        if raw.is_array() && q.columns.is_none() {
            let _: Vec<T> = serde_json::from_value(raw.clone())
                .map_err(|e| CoreError::new(ErrorCode::ManagementApi, format!("{path}: {e}")))?;
        }
        Ok(raw)
    }

    // ---------------------------------------------------------------- cluster

    pub async fn overview(&self) -> Result<Overview> {
        self.get("overview").await
    }
    pub async fn whoami(&self) -> Result<Value> {
        self.get("whoami").await
    }
    pub async fn cluster_name(&self) -> Result<Value> {
        self.get("cluster-name").await
    }
    pub async fn nodes(&self) -> Result<Vec<NodeInfo>> {
        self.get("nodes").await
    }
    pub async fn node(&self, name: &str) -> Result<NodeInfo> {
        self.get(&format!("nodes/{}?memory=true", pct(name))).await
    }
    pub async fn feature_flags(&self) -> Result<Vec<FeatureFlag>> {
        self.get("feature-flags").await
    }
    pub async fn deprecated_features_used(&self) -> Result<Value> {
        self.get("deprecated-features/used").await
    }
    pub async fn extensions(&self) -> Result<Value> {
        self.get("extensions").await
    }

    /// Run the documented health checks and collect results without failing
    /// on the first unhealthy one.
    pub async fn health(&self) -> Result<Value> {
        let checks = [
            ("alarms", "health/checks/alarms"),
            ("local_alarms", "health/checks/local-alarms"),
            ("virtual_hosts", "health/checks/virtual-hosts"),
            ("port_listener_amqp", "health/checks/port-listener/5672"),
            (
                "protocol_listener_amqp",
                "health/checks/protocol-listener/amqp",
            ),
            ("quorum_critical", "health/checks/node-is-quorum-critical"),
            (
                "certificate_expiration",
                "health/checks/certificate-expiration/1/months",
            ),
        ];
        let mut out = Map::new();
        for (key, path) in checks {
            let r = self
                .http
                .get(self.url(path))
                .basic_auth(&self.user, Some(self.password.expose()))
                .send()
                .await;
            let entry = match r {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body: Value = resp.json().await.unwrap_or(Value::Null);
                    json!({ "ok": status == 200, "http_status": status, "body": body })
                }
                Err(e) => {
                    json!({ "ok": false, "error": rabbitmq_core::redact::redact_text(&e.to_string()) })
                }
            };
            out.insert(key.into(), entry);
        }
        Ok(Value::Object(out))
    }

    // ---------------------------------------------------------------- vhosts

    pub async fn vhosts(&self) -> Result<Vec<VhostInfo>> {
        self.get("vhosts").await
    }
    pub async fn vhost(&self, name: &str) -> Result<VhostInfo> {
        self.get(&format!("vhosts/{}", v(name))).await
    }
    pub async fn create_vhost(&self, name: &str, body: &Value) -> Result<()> {
        self.put(&format!("vhosts/{}", v(name)), body).await
    }
    pub async fn delete_vhost(&self, name: &str) -> Result<()> {
        self.delete(&format!("vhosts/{}", v(name)), &[]).await
    }

    // ---------------------------------------------------------------- queues

    pub async fn queues(&self, vhost: Option<&str>, q: &ListQuery) -> Result<Value> {
        let path = match vhost {
            Some(vh) => format!("queues/{}", v(vh)),
            None => "queues".into(),
        };
        self.list::<QueueInfo>(&path, q).await
    }
    pub async fn queue(&self, vhost: &str, name: &str) -> Result<QueueInfo> {
        self.get(&format!("queues/{}/{}", v(vhost), pct(name)))
            .await
    }
    pub async fn declare_queue(&self, vhost: &str, name: &str, body: &Value) -> Result<()> {
        self.put(&format!("queues/{}/{}", v(vhost), pct(name)), body)
            .await
    }
    pub async fn delete_queue(
        &self,
        vhost: &str,
        name: &str,
        if_empty: bool,
        if_unused: bool,
    ) -> Result<()> {
        let mut q = Vec::new();
        if if_empty {
            q.push(("if-empty".to_string(), "true".to_string()));
        }
        if if_unused {
            q.push(("if-unused".to_string(), "true".to_string()));
        }
        self.delete(&format!("queues/{}/{}", v(vhost), pct(name)), &q)
            .await
    }
    pub async fn purge_queue(&self, vhost: &str, name: &str) -> Result<()> {
        self.delete(&format!("queues/{}/{}/contents", v(vhost), pct(name)), &[])
            .await
    }
    pub async fn queue_bindings(&self, vhost: &str, name: &str) -> Result<Vec<BindingInfo>> {
        self.get(&format!("queues/{}/{}/bindings", v(vhost), pct(name)))
            .await
    }

    // ---------------------------------------------------------------- exchanges

    pub async fn exchanges(&self, vhost: Option<&str>, q: &ListQuery) -> Result<Value> {
        let path = match vhost {
            Some(vh) => format!("exchanges/{}", v(vh)),
            None => "exchanges".into(),
        };
        self.list::<ExchangeInfo>(&path, q).await
    }
    pub async fn exchange(&self, vhost: &str, name: &str) -> Result<ExchangeInfo> {
        self.get(&format!("exchanges/{}/{}", v(vhost), pct(name)))
            .await
    }
    pub async fn declare_exchange(&self, vhost: &str, name: &str, body: &Value) -> Result<()> {
        self.put(&format!("exchanges/{}/{}", v(vhost), pct(name)), body)
            .await
    }
    pub async fn delete_exchange(&self, vhost: &str, name: &str, if_unused: bool) -> Result<()> {
        let q = if if_unused {
            vec![("if-unused".to_string(), "true".to_string())]
        } else {
            vec![]
        };
        self.delete(&format!("exchanges/{}/{}", v(vhost), pct(name)), &q)
            .await
    }

    // ---------------------------------------------------------------- bindings

    pub async fn bindings(&self, vhost: Option<&str>) -> Result<Vec<BindingInfo>> {
        match vhost {
            Some(vh) => self.get(&format!("bindings/{}", v(vh))).await,
            None => self.get("bindings").await,
        }
    }
    pub async fn bindings_from(&self, vhost: &str, exchange: &str) -> Result<Vec<BindingInfo>> {
        self.get(&format!(
            "exchanges/{}/{}/bindings/source",
            v(vhost),
            pct(exchange)
        ))
        .await
    }
    pub async fn bindings_to_exchange(
        &self,
        vhost: &str,
        exchange: &str,
    ) -> Result<Vec<BindingInfo>> {
        self.get(&format!(
            "exchanges/{}/{}/bindings/destination",
            v(vhost),
            pct(exchange)
        ))
        .await
    }
    pub async fn create_binding(
        &self,
        vhost: &str,
        source: &str,
        destination_type: &str,
        destination: &str,
        routing_key: &str,
        arguments: &Map<String, Value>,
    ) -> Result<()> {
        let d = if destination_type == "exchange" {
            "e"
        } else {
            "q"
        };
        self.post(
            &format!(
                "bindings/{}/e/{}/{}/{}",
                v(vhost),
                pct(source),
                d,
                pct(destination)
            ),
            &json!({ "routing_key": routing_key, "arguments": arguments }),
        )
        .await
        .map(|_| ())
    }
    pub async fn delete_binding(
        &self,
        vhost: &str,
        source: &str,
        destination_type: &str,
        destination: &str,
        properties_key: &str,
    ) -> Result<()> {
        let d = if destination_type == "exchange" {
            "e"
        } else {
            "q"
        };
        self.delete(
            &format!(
                "bindings/{}/e/{}/{}/{}/{}",
                v(vhost),
                pct(source),
                d,
                pct(destination),
                pct(properties_key)
            ),
            &[],
        )
        .await
    }

    /// Build a [`Topology`] snapshot for routing simulation and graphing.
    pub async fn topology(&self, vhost: &str) -> Result<Topology> {
        let q = ListQuery {
            disable_stats: true,
            ..Default::default()
        };
        let ex: Vec<ExchangeInfo> = serde_json::from_value(self.exchanges(Some(vhost), &q).await?)
            .map_err(|e| CoreError::new(ErrorCode::ManagementApi, e.to_string()))?;
        let qs: Vec<QueueInfo> = serde_json::from_value(self.queues(Some(vhost), &q).await?)
            .map_err(|e| CoreError::new(ErrorCode::ManagementApi, e.to_string()))?;
        let bs = self.bindings(Some(vhost)).await?;
        Ok(Topology {
            exchanges: ex
                .into_iter()
                .map(|e| Exchange {
                    name: e.name,
                    vhost: e.vhost,
                    kind: e.kind,
                    durable: e.durable,
                    auto_delete: e.auto_delete,
                    internal: e.internal,
                    arguments: e.arguments,
                    policy: e.policy,
                })
                .collect(),
            queues: qs
                .into_iter()
                .map(|q| Queue {
                    kind: q.queue_type().to_string(),
                    name: q.name,
                    vhost: q.vhost,
                    durable: q.durable,
                    auto_delete: q.auto_delete,
                    exclusive: q.exclusive.unwrap_or(false),
                    arguments: q.arguments,
                    policy: q.policy,
                })
                .collect(),
            bindings: bs
                .into_iter()
                .map(|b| Binding {
                    source: b.source,
                    vhost: b.vhost,
                    destination: b.destination,
                    destination_type: b.destination_type,
                    routing_key: b.routing_key,
                    arguments: b.arguments,
                })
                .collect(),
        })
    }

    // ---------------------------------------------------------------- connections

    pub async fn connections(&self, q: &ListQuery) -> Result<Value> {
        self.list::<ConnectionInfo>("connections", q).await
    }
    pub async fn connection(&self, name: &str) -> Result<ConnectionInfo> {
        self.get(&format!("connections/{}", pct(name))).await
    }
    pub async fn close_connection(&self, name: &str, reason: &str) -> Result<()> {
        let req = self
            .http
            .delete(self.url(&format!("connections/{}", pct(name))))
            .header("X-Reason", reason);
        self.send(req, "DELETE connection", true).await.map(|_| ())
    }
    pub async fn channels(&self, q: &ListQuery) -> Result<Value> {
        self.list::<ChannelInfo>("channels", q).await
    }
    pub async fn connection_channels(&self, name: &str) -> Result<Vec<ChannelInfo>> {
        self.get(&format!("connections/{}/channels", pct(name)))
            .await
    }
    pub async fn consumers(&self, vhost: Option<&str>) -> Result<Vec<ConsumerInfo>> {
        match vhost {
            Some(vh) => self.get(&format!("consumers/{}", v(vh))).await,
            None => self.get("consumers").await,
        }
    }

    // ---------------------------------------------------------------- users & permissions

    pub async fn users(&self) -> Result<Vec<UserInfo>> {
        self.get("users").await
    }
    pub async fn user(&self, name: &str) -> Result<UserInfo> {
        self.get(&format!("users/{}", pct(name))).await
    }
    /// Create or update a user. `password` is sent once and never stored.
    pub async fn put_user(&self, name: &str, password: Option<&str>, tags: &str) -> Result<()> {
        let mut body = json!({ "tags": tags });
        match password {
            Some(p) => body["password"] = Value::String(p.into()),
            None => body["password_hash"] = Value::String(String::new()),
        }
        self.put(&format!("users/{}", pct(name)), &body).await
    }
    pub async fn delete_user(&self, name: &str) -> Result<()> {
        self.delete(&format!("users/{}", pct(name)), &[]).await
    }
    pub async fn permissions(&self) -> Result<Vec<Permission>> {
        self.get("permissions").await
    }
    pub async fn user_permissions(&self, user: &str) -> Result<Vec<Permission>> {
        self.get(&format!("users/{}/permissions", pct(user))).await
    }
    pub async fn set_permissions(
        &self,
        vhost: &str,
        user: &str,
        configure: &str,
        write: &str,
        read: &str,
    ) -> Result<()> {
        self.put(
            &format!("permissions/{}/{}", v(vhost), pct(user)),
            &json!({ "configure": configure, "write": write, "read": read }),
        )
        .await
    }
    pub async fn clear_permissions(&self, vhost: &str, user: &str) -> Result<()> {
        self.delete(&format!("permissions/{}/{}", v(vhost), pct(user)), &[])
            .await
    }
    pub async fn topic_permissions(&self) -> Result<Vec<TopicPermission>> {
        self.get("topic-permissions").await
    }
    pub async fn set_topic_permissions(
        &self,
        vhost: &str,
        user: &str,
        exchange: &str,
        write: &str,
        read: &str,
    ) -> Result<()> {
        self.put(
            &format!("topic-permissions/{}/{}", v(vhost), pct(user)),
            &json!({ "exchange": exchange, "write": write, "read": read }),
        )
        .await
    }
    pub async fn clear_topic_permissions(&self, vhost: &str, user: &str) -> Result<()> {
        self.delete(
            &format!("topic-permissions/{}/{}", v(vhost), pct(user)),
            &[],
        )
        .await
    }

    // ---------------------------------------------------------------- policies, parameters, limits

    pub async fn policies(&self, vhost: Option<&str>) -> Result<Vec<Policy>> {
        match vhost {
            Some(vh) => self.get(&format!("policies/{}", v(vh))).await,
            None => self.get("policies").await,
        }
    }
    pub async fn put_policy(&self, p: &Policy) -> Result<()> {
        self.put(
            &format!("policies/{}/{}", v(&p.vhost), pct(&p.name)),
            &json!({ "pattern": p.pattern, "apply-to": p.apply_to, "definition": p.definition, "priority": p.priority }),
        )
        .await
    }
    pub async fn delete_policy(&self, vhost: &str, name: &str) -> Result<()> {
        self.delete(&format!("policies/{}/{}", v(vhost), pct(name)), &[])
            .await
    }
    pub async fn operator_policies(&self, vhost: Option<&str>) -> Result<Vec<Policy>> {
        match vhost {
            Some(vh) => self.get(&format!("operator-policies/{}", v(vh))).await,
            None => self.get("operator-policies").await,
        }
    }
    pub async fn put_operator_policy(&self, p: &Policy) -> Result<()> {
        self.put(
            &format!("operator-policies/{}/{}", v(&p.vhost), pct(&p.name)),
            &json!({ "pattern": p.pattern, "apply-to": p.apply_to, "definition": p.definition, "priority": p.priority }),
        )
        .await
    }
    pub async fn delete_operator_policy(&self, vhost: &str, name: &str) -> Result<()> {
        self.delete(
            &format!("operator-policies/{}/{}", v(vhost), pct(name)),
            &[],
        )
        .await
    }
    pub async fn parameters(&self, component: Option<&str>) -> Result<Value> {
        match component {
            Some(c) => self.get(&format!("parameters/{}", pct(c))).await,
            None => self.get("parameters").await,
        }
    }
    pub async fn global_parameters(&self) -> Result<Value> {
        self.get("global-parameters").await
    }
    pub async fn vhost_limits(&self, vhost: Option<&str>) -> Result<Value> {
        match vhost {
            Some(vh) => self.get(&format!("vhost-limits/{}", v(vh))).await,
            None => self.get("vhost-limits").await,
        }
    }
    pub async fn set_vhost_limit(&self, vhost: &str, limit: &str, value: i64) -> Result<()> {
        self.put(
            &format!("vhost-limits/{}/{}", v(vhost), pct(limit)),
            &json!({ "value": value }),
        )
        .await
    }
    pub async fn clear_vhost_limit(&self, vhost: &str, limit: &str) -> Result<()> {
        self.delete(&format!("vhost-limits/{}/{}", v(vhost), pct(limit)), &[])
            .await
    }
    pub async fn user_limits(&self) -> Result<Value> {
        self.get("user-limits").await
    }

    // ---------------------------------------------------------------- definitions

    pub async fn export_definitions(&self, vhost: Option<&str>) -> Result<Value> {
        match vhost {
            Some(vh) => self.get(&format!("definitions/{}", v(vh))).await,
            None => self.get("definitions").await,
        }
    }
    pub async fn import_definitions(&self, vhost: Option<&str>, defs: &Value) -> Result<()> {
        let path = match vhost {
            Some(vh) => format!("definitions/{}", v(vh)),
            None => "definitions".into(),
        };
        self.post(&path, defs).await.map(|_| ())
    }

    // ---------------------------------------------------------------- streams (management plugin view)

    pub async fn stream_connections(&self) -> Result<Value> {
        self.get("stream/connections").await
    }
    pub async fn stream_publishers(&self, vhost: Option<&str>) -> Result<Value> {
        match vhost {
            Some(vh) => self.get(&format!("stream/publishers/{}", v(vh))).await,
            None => self.get("stream/publishers").await,
        }
    }
    pub async fn stream_consumers(&self, vhost: Option<&str>) -> Result<Value> {
        match vhost {
            Some(vh) => self.get(&format!("stream/consumers/{}", v(vh))).await,
            None => self.get("stream/consumers").await,
        }
    }
}

/// Convert exported definitions to a [`Topology`] (used to diff a cluster
/// against a definitions file).
pub fn topology_from_definitions(defs: &Value, vhost: Option<&str>) -> Topology {
    let keep = |o: &Value| vhost.is_none_or(|vh| o["vhost"].as_str() == Some(vh));
    let arr = |k: &str| defs[k].as_array().cloned().unwrap_or_default();
    Topology {
        exchanges: arr("exchanges")
            .into_iter()
            .filter(keep)
            .filter_map(|e| serde_json::from_value(e).ok())
            .collect(),
        queues: arr("queues")
            .into_iter()
            .filter(keep)
            .filter_map(|mut q| {
                if q.get("type").is_none() {
                    let t = q["arguments"]["x-queue-type"]
                        .as_str()
                        .unwrap_or("classic")
                        .to_string();
                    q["type"] = Value::String(t);
                }
                serde_json::from_value(q).ok()
            })
            .collect(),
        bindings: arr("bindings")
            .into_iter()
            .filter(keep)
            .filter_map(|b| serde_json::from_value(b).ok())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_query_params() {
        let q = ListQuery {
            page: Some(2),
            name: Some("^orders".into()),
            use_regex: true,
            columns: Some(vec!["name".into(), "messages".into()]),
            disable_stats: true,
            ..Default::default()
        };
        let p = q.params();
        assert!(p.contains(&("page".into(), "2".into())));
        assert!(p.contains(&("page_size".into(), "100".into())));
        assert!(p.contains(&("use_regex".into(), "true".into())));
        assert!(p.contains(&("columns".into(), "name,messages".into())));
    }

    #[test]
    fn queue_info_preserves_extra_fields() {
        let q: QueueInfo = serde_json::from_value(json!({
            "name": "q", "vhost": "/", "type": "quorum", "messages": 3, "future_field": 42
        }))
        .unwrap();
        assert_eq!(q.queue_type(), "quorum");
        assert_eq!(q.extra["future_field"], 42);
        let back = serde_json::to_value(&q).unwrap();
        assert_eq!(back["future_field"], 42);
    }

    #[test]
    fn user_password_hash_not_serialized() {
        let u: UserInfo =
            serde_json::from_value(json!({"name": "a", "tags": [], "password_hash": "h"})).unwrap();
        assert!(serde_json::to_value(&u)
            .unwrap()
            .get("password_hash")
            .is_none());
    }

    #[test]
    fn definitions_to_topology() {
        let defs = json!({
            "queues": [{"name": "q", "vhost": "/", "durable": true, "arguments": {"x-queue-type": "quorum"}},
                       {"name": "o", "vhost": "other", "durable": true, "arguments": {}}],
            "exchanges": [{"name": "x", "vhost": "/", "type": "topic", "durable": true, "auto_delete": false, "internal": false, "arguments": {}}],
            "bindings": [{"source": "x", "vhost": "/", "destination": "q", "destination_type": "queue", "routing_key": "#", "arguments": {}}]
        });
        let t = topology_from_definitions(&defs, Some("/"));
        assert_eq!(t.queues.len(), 1);
        assert_eq!(t.queues[0].kind, "quorum");
        assert_eq!(t.simulate("x", "a.b", &Map::new()).queues, vec!["q"]);
    }

    #[test]
    fn missing_management_url() {
        let mut p = Profile::local("x");
        p.management_url = None;
        assert_eq!(
            ManagementClient::from_profile(&p).unwrap_err().code,
            ErrorCode::Unsupported
        );
    }
}
