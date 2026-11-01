//! # rabbitmq-testkit
//!
//! Helpers for integration tests against a real, isolated broker started by
//! `scripts/test-broker.sh`. Tests call [`broker`] and skip (print + return)
//! when no broker is reachable, so `cargo test` stays green on machines
//! without RabbitMQ while CI runs the full suite with `RMQ_REQUIRE_BROKER=1`.

use rabbitmq_core::profile::Profile;
use std::sync::atomic::{AtomicU64, Ordering};

/// Profile for the isolated test broker. Override with `RMQ_TEST_HOST`,
/// `RMQ_TEST_AMQP_PORT`, `RMQ_TEST_MGMT_URL`, `RMQ_TEST_STREAM_PORT`.
pub fn test_profile() -> Profile {
    let mut p = Profile::local("test");
    if let Ok(h) = std::env::var("RMQ_TEST_HOST") {
        p.host = h;
    }
    if let Some(port) = std::env::var("RMQ_TEST_AMQP_PORT").ok().and_then(|v| v.parse().ok()) {
        p.amqp_port = port;
    }
    if let Some(port) = std::env::var("RMQ_TEST_STREAM_PORT").ok().and_then(|v| v.parse().ok()) {
        p.stream_port = port;
    }
    p.management_url = Some(
        std::env::var("RMQ_TEST_MGMT_URL").unwrap_or_else(|_| format!("http://{}:15672", p.host)),
    );
    p
}

/// Return the test profile if a broker is reachable, else `None` (or panic
/// when `RMQ_REQUIRE_BROKER=1`).
pub async fn broker() -> Option<Profile> {
    let p = test_profile();
    let addr = format!("{}:{}", p.host, p.amqp_port);
    let ok = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false);
    if ok {
        Some(p)
    } else if std::env::var("RMQ_REQUIRE_BROKER").as_deref() == Ok("1") {
        panic!("RMQ_REQUIRE_BROKER=1 but no broker at {addr}; run scripts/test-broker.sh start");
    } else {
        eprintln!("skipping: no RabbitMQ broker at {addr}");
        None
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Unique, recognisable resource name for a test. All test resources start
/// with `rmqel-test.` so leftovers are easy to find and clean.
pub fn unique(prefix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("rmqel-test.{prefix}.{}.{t:x}.{n}", std::process::id())
}

/// Minimal admin helper using the Management API directly (independent of the
/// code under test) for fixtures and assertions.
pub struct Admin {
    base: String,
    http: reqwest::Client,
}

impl Admin {
    pub fn new(p: &Profile) -> Self {
        Self {
            base: p.management_url.clone().unwrap_or_default(),
            http: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/{}", self.base.trim_end_matches('/'), path)
    }

    pub async fn get(&self, path: &str) -> serde_json::Value {
        self.http
            .get(self.url(path))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .expect("mgmt get")
            .json()
            .await
            .unwrap_or(serde_json::Value::Null)
    }

    pub async fn put(&self, path: &str, body: serde_json::Value) -> u16 {
        self.http
            .put(self.url(path))
            .basic_auth("guest", Some("guest"))
            .json(&body)
            .send()
            .await
            .expect("mgmt put")
            .status()
            .as_u16()
    }

    pub async fn post(&self, path: &str, body: serde_json::Value) -> u16 {
        self.http
            .post(self.url(path))
            .basic_auth("guest", Some("guest"))
            .json(&body)
            .send()
            .await
            .expect("mgmt post")
            .status()
            .as_u16()
    }

    pub async fn delete(&self, path: &str) -> u16 {
        self.http
            .delete(self.url(path))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .map(|r| r.status().as_u16())
            .unwrap_or(0)
    }

    pub async fn delete_queue(&self, name: &str) {
        self.delete(&format!("queues/%2F/{}", pct(name))).await;
    }

    pub async fn delete_exchange(&self, name: &str) {
        self.delete(&format!("exchanges/%2F/{}", pct(name))).await;
    }

    /// Queue message count as seen by the broker. Management stats are
    /// eventually consistent, so poll until `pred` holds or time out.
    pub async fn wait_queue<F: Fn(&serde_json::Value) -> bool>(&self, name: &str, pred: F) -> serde_json::Value {
        let mut last = serde_json::Value::Null;
        for _ in 0..60 {
            last = self.get(&format!("queues/%2F/{}", pct(name))).await;
            if pred(&last) {
                return last;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        panic!("queue {name} never satisfied predicate; last = {last}");
    }

    /// Force-close all connections whose client-provided name matches.
    pub async fn close_connections_named(&self, name: &str) -> usize {
        let conns = self.get("connections").await;
        let mut n = 0;
        for c in conns.as_array().cloned().unwrap_or_default() {
            let cname = c["client_properties"]["connection_name"].as_str().unwrap_or("");
            if cname == name {
                if let Some(id) = c["name"].as_str() {
                    self.delete(&format!("connections/{}", pct(id))).await;
                    n += 1;
                }
            }
        }
        n
    }

    /// Wait until a connection with this client-provided name is visible.
    pub async fn wait_connection(&self, name: &str) {
        for _ in 0..60 {
            let conns = self.get("connections").await;
            if conns
                .as_array()
                .map(|a| a.iter().any(|c| c["client_properties"]["connection_name"] == name))
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        panic!("connection {name} never appeared");
    }
}

pub fn pct(s: &str) -> String {
    rabbitmq_core::profile::pct(s)
}
