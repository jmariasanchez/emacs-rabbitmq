//! End-to-end tests: spawn the real `rabbitmq-bridge` binary and talk to it
//! over stdio exactly as Emacs does.

use rabbitmq_protocol::{read_frame, write_frame};
use rabbitmq_testkit::{broker, unique, Admin};
use serde_json::{json, Value};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

struct Bridge {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next: i64,
    /// Notifications seen while waiting for responses.
    notes: Vec<Value>,
}

impl Bridge {
    async fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rabbitmq-bridge"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn bridge");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self { child, stdin, stdout, next: 0, notes: Vec::new() }
    }

    async fn send(&mut self, v: &Value) {
        write_frame(&mut self.stdin, &serde_json::to_vec(v).unwrap()).await.unwrap();
    }

    async fn request_id(&mut self, method: &str, params: Value) -> i64 {
        self.next += 1;
        let id = self.next;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})).await;
        id
    }

    async fn wait(&mut self, id: i64) -> Value {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(30), read_frame(&mut self.stdout))
                .await
                .expect("timed out waiting for bridge")
                .expect("bridge frame");
            let v: Value = serde_json::from_slice(&frame).unwrap();
            if v.get("id") == Some(&json!(id)) {
                return v;
            }
            if v.get("method").is_some() {
                self.notes.push(v);
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.request_id(method, params).await;
        self.wait(id).await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let r = self.call(method, params.clone()).await;
        assert!(r.get("error").is_none(), "{method} {params} failed: {r}");
        r["result"].clone()
    }

    async fn err(&mut self, method: &str, params: Value) -> Value {
        let r = self.call(method, params.clone()).await;
        assert!(r.get("error").is_some(), "{method} {params} unexpectedly succeeded: {r}");
        r["error"].clone()
    }
}

fn profile(read_only: bool) -> Value {
    let p = rabbitmq_testkit::test_profile();
    json!({
        "name": "e2e", "host": p.host, "amqp_port": p.amqp_port, "stream_port": p.stream_port,
        "management_url": p.management_url, "username": "guest", "password": "guest",
        "read_only": read_only,
        "protected": [{"kind": "queue", "name": "rmqel-test.protected.*", "protection": "no_destructive"}]
    })
}

#[tokio::test]
async fn handshake_and_unknown_method() {
    let mut b = Bridge::spawn().await;
    let hello = b.ok("bridge.hello", json!({})).await;
    assert_eq!(hello["protocol_version"], 1);
    let methods = b.ok("bridge.methods", json!({})).await;
    assert!(methods.as_array().unwrap().iter().any(|m| m["name"] == "message.get" && m["effect"] == "consume"));
    let e = b.err("no.such.method", json!({})).await;
    assert_eq!(e["code"], -32601);
    // Garbage JSON yields a parse error, not a crash.
    write_frame(&mut b.stdin, b"{not json").await.unwrap();
    let frame = read_frame(&mut b.stdout).await.unwrap();
    let v: Value = serde_json::from_slice(&frame).unwrap();
    assert_eq!(v["error"]["code"], -32700);
    assert!(b.ok("bridge.ping", json!({})).await["pong"].as_bool().unwrap());
}

#[tokio::test]
async fn full_workflow_with_safety_gates_and_audit() {
    if broker().await.is_none() {
        return;
    }
    let mut b = Bridge::spawn().await;
    let sid = unique("sess");
    let info = b.ok("session.open", json!({"session": sid, "profile": profile(false)})).await;
    assert_eq!(info["profile"]["password"], "********", "secrets must never be echoed");
    assert!(info["features"]["native_amqp10"].as_bool().unwrap());

    let q = unique("q");
    b.ok("queue.declare", json!({"session": sid, "name": q, "type": "quorum"})).await;
    let pubr = b
        .ok("message.publish", json!({"session": sid, "messages": [
            {"routing_key": q, "payload": {"data": "one"}},
            {"routing_key": q, "payload": {"encoding": "base64", "data": "/wD+"}}
        ]}))
        .await;
    assert_eq!(pubr["summary"]["confirmed"], 2);

    // Consuming from an ordinary queue requires explicit consent.
    let e = b.err("message.get", json!({"session": sid, "queue": q})).await;
    assert_eq!(e["data"]["code"], "confirmation_required");
    assert_eq!(e["data"]["detail"]["requires"], "consent");
    let got = b.ok("message.get", json!({"session": sid, "queue": q, "count": 2, "consent": true})).await;
    assert_eq!(got["destructive"], true);
    assert_eq!(got["messages"][0]["payload"]["data"], "one");
    assert_eq!(got["messages"][1]["payload"]["encoding"], "base64");
    let ids: Vec<Value> = got["messages"].as_array().unwrap().iter().map(|m| m["id"].clone()).collect();
    let st = b.ok("message.settle", json!({"session": sid, "ids": ids, "outcome": "ack"})).await;
    assert_eq!(st["settled"].as_array().unwrap().len(), 2);

    // Routing prediction is labelled as such.
    let sim = b.ok("routing.simulate", json!({"session": sid, "exchange": "", "routing_key": q})).await;
    assert_eq!(sim["prediction"], true);
    assert_eq!(sim["queues"], json!([q]));

    // Destructive ops need the exact name typed back.
    let e = b.err("queue.delete", json!({"session": sid, "name": q})).await;
    assert_eq!(e["data"]["detail"]["expected"], q);
    let e = b.err("queue.delete", json!({"session": sid, "name": q, "confirm": "wrong"})).await;
    assert_eq!(e["data"]["code"], "confirmation_required");
    b.ok("queue.delete", json!({"session": sid, "name": q, "confirm": q})).await;

    // Protected resources reject destructive operations even when confirmed.
    let pq = format!("rmqel-test.protected.{}", std::process::id());
    b.ok("queue.declare", json!({"session": sid, "name": pq})).await;
    let e = b.err("queue.purge", json!({"session": sid, "name": pq, "confirm": pq})).await;
    assert_eq!(e["data"]["code"], "protected_resource");
    Admin::new(&rabbitmq_testkit::test_profile()).delete_queue(&pq).await;

    // Audit log records interventions with redacted params.
    let audit = b.ok("bridge.audit", json!({})).await;
    let audit = audit.as_array().unwrap();
    assert!(audit.iter().any(|r| r["method"] == "queue.delete" && r["outcome"] == "succeeded"));
    assert!(audit.iter().any(|r| r["method"] == "message.get" && r["outcome"] == "denied"));
    assert!(audit.iter().any(|r| r["method"] == "queue.purge" && r["outcome"] == "denied"));
    assert!(audit.iter().all(|r| r["params"]["messages"].as_array().is_none_or(|m| m.iter().all(|x| x["payload"]["omitted"] == true))));

    // Diagnostics contain no secrets.
    let diag = b.ok("bridge.diagnostics", json!({})).await.to_string();
    assert!(!diag.contains("\"guest\",\"read_only\"") && !diag.contains(":guest@"), "{diag}");

    b.ok("session.close", json!({"session": sid})).await;
    let e = b.err("session.info", json!({"session": sid})).await;
    assert_eq!(e["data"]["code"], "not_found");
}

#[tokio::test]
async fn read_only_profile_blocks_interventions_but_allows_observation() {
    if broker().await.is_none() {
        return;
    }
    let mut b = Bridge::spawn().await;
    let sid = unique("ro");
    b.ok("session.open", json!({"session": sid, "profile": profile(true)})).await;
    for (m, p) in [
        ("queue.declare", json!({"name": unique("x")})),
        ("message.publish", json!({"messages": [{"routing_key": "x", "payload": {"data": "x"}}]})),
        ("message.get", json!({"queue": "x", "consent": true})),
        ("mgmt.user.put", json!({"name": unique("u"), "password": "x"})),
    ] {
        let mut p = p;
        p["session"] = json!(sid);
        let e = b.err(m, p).await;
        assert_eq!(e["data"]["code"], "read_only_profile", "{m}");
    }
    let o = b.ok("mgmt.overview", json!({"session": sid})).await;
    assert!(o["fetched_at"].as_u64().is_some(), "management data must carry a sample time");
    assert_eq!(o["source"], "management_api");
    b.ok("mgmt.nodes", json!({"session": sid})).await;
    b.ok("topology.snapshot", json!({"session": sid})).await;
}

#[tokio::test]
async fn consumer_notifications_and_stream_replay_over_rpc() {
    if broker().await.is_none() {
        return;
    }
    let mut b = Bridge::spawn().await;
    let sid = unique("cons");
    b.ok("session.open", json!({"session": sid, "profile": profile(false)})).await;
    let q = unique("q");
    b.ok("queue.declare", json!({"session": sid, "name": q})).await;
    let c = b.ok("consumer.start", json!({"session": sid, "queue": q, "consent": true, "flush_ms": 20})).await;
    b.ok("message.publish", json!({"session": sid, "messages": [{"routing_key": q, "payload": {"data": "live"}}]})).await;
    // Wait for a $/deliveries notification.
    let mut delivered = None;
    for _ in 0..30 {
        b.ok("bridge.ping", json!({})).await;
        if let Some(n) = b.notes.iter().find(|n| n["method"] == "$/deliveries") {
            delivered = Some(n.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let n = delivered.expect("deliveries notification");
    assert_eq!(n["params"]["messages"][0]["payload"]["data"], "live");
    let id = n["params"]["messages"][0]["id"].clone();
    b.ok("message.settle", json!({"session": sid, "id": id, "outcome": "ack"})).await;
    b.ok("consumer.cancel", json!({"session": sid, "consumer": c["tag"]})).await;
    b.ok("queue.delete", json!({"session": sid, "name": q, "confirm": q})).await;

    // Streams: replay without consent (non-destructive).
    let st = unique("st");
    b.ok("stream.create", json!({"session": sid, "name": st})).await;
    b.ok("stream.publish", json!({"session": sid, "stream": st, "messages": [{"payload": {"data": "s1"}}, {"payload": {"data": "s2"}}]})).await;
    let r = b.ok("stream.read", json!({"session": sid, "stream": st, "start": {"type": "first"}})).await;
    assert_eq!(r["destructive"], false);
    assert_eq!(r["messages"].as_array().unwrap().len(), 2);
    b.ok("stream.delete", json!({"session": sid, "name": st, "confirm": st})).await;
}

#[tokio::test]
async fn cancellation_reports_cancelled() {
    if broker().await.is_none() {
        return;
    }
    let mut b = Bridge::spawn().await;
    let sid = unique("cancel");
    b.ok("session.open", json!({"session": sid, "profile": profile(false)})).await;
    let st = unique("idle");
    b.ok("stream.create", json!({"session": sid, "name": st})).await;
    // A read on an empty stream with a long idle timeout stays in flight.
    let id = b.request_id("stream.read", json!({"session": sid, "stream": st, "start": {"type": "next"}, "idle_ms": 20000})).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    b.send(&json!({"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": id}})).await;
    let r = b.wait(id).await;
    assert_eq!(r["error"]["code"], -32800, "{r}");
    // Observation cancel is not ambiguous.
    assert_eq!(r["error"]["data"]["ambiguous"], false);
    b.ok("stream.delete", json!({"session": sid, "name": st, "confirm": st})).await;
}

#[tokio::test]
async fn stdin_close_releases_broker_connections() {
    let Some(p) = broker().await else { return };
    let admin = Admin::new(&p);
    let mut b = Bridge::spawn().await;
    let sid = unique("eof");
    let mut prof = profile(false);
    prof["connection_name"] = json!(sid);
    b.ok("session.open", json!({"session": sid, "profile": prof})).await;
    admin.wait_connection(&sid).await;
    drop(b.stdin);
    let status = tokio::time::timeout(Duration::from_secs(15), b.child.wait()).await.expect("bridge exits").unwrap();
    assert!(status.success());
    for _ in 0..40 {
        let conns = admin.get("connections").await;
        if !conns.as_array().unwrap().iter().any(|c| c["client_properties"]["connection_name"] == sid.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("bridge exited but left connection {sid} open");
}
