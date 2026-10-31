//! Topology model, routing prediction, and topology diffing.
//!
//! Routing simulation operates on a *snapshot* of known bindings (usually
//! from the Management API). Results are predictions: the broker may differ
//! because of concurrent topology changes, policies not visible to the
//! user, plugin exchange types, queue length limits, or permissions. Every
//! [`RoutePrediction`] carries the caveats that applied.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    pub name: String,
    #[serde(default)]
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
    /// Effective policy name, if any (from Management API).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub name: String,
    #[serde(default)]
    pub vhost: String,
    /// `classic`, `quorum`, or `stream`.
    #[serde(rename = "type", default = "classic")]
    pub kind: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

fn classic() -> String {
    "classic".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub source: String,
    #[serde(default)]
    pub vhost: String,
    pub destination: String,
    /// `queue` or `exchange`.
    pub destination_type: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default)]
    pub arguments: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Topology {
    #[serde(default)]
    pub exchanges: Vec<Exchange>,
    #[serde(default)]
    pub queues: Vec<Queue>,
    #[serde(default)]
    pub bindings: Vec<Binding>,
}

/// One hop in a predicted route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteHop {
    pub exchange: String,
    pub destination: String,
    pub destination_type: String,
    /// Why this binding matched (`direct key "a"`, `topic "a.*"`, ...).
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutePrediction {
    /// Always true: this is a prediction from known bindings, not a broker confirmation.
    pub prediction: bool,
    pub queues: Vec<String>,
    pub hops: Vec<RouteHop>,
    /// Exchanges visited (in order), including alternate exchanges.
    pub exchanges_visited: Vec<String>,
    /// True when no queue was reached: with `mandatory` the broker would
    /// return the message, otherwise drop it.
    pub unroutable: bool,
    /// Alternate exchanges that were used.
    pub alternate_exchanges_used: Vec<String>,
    pub caveats: Vec<String>,
}

const BASE_CAVEAT: &str = "Prediction from a snapshot of known bindings; concurrent topology changes, policies, permissions, queue limits, and plugin behaviour can change actual delivery.";

impl Topology {
    pub fn exchange(&self, name: &str) -> Option<&Exchange> {
        self.exchanges.iter().find(|e| e.name == name)
    }

    pub fn queue(&self, name: &str) -> Option<&Queue> {
        self.queues.iter().find(|q| q.name == name)
    }

    /// Predict where a message would be routed.
    pub fn simulate(
        &self,
        exchange: &str,
        routing_key: &str,
        headers: &Map<String, Value>,
    ) -> RoutePrediction {
        let mut caveats = vec![BASE_CAVEAT.to_string()];
        let mut hops = Vec::new();
        let mut queues = BTreeSet::new();
        let mut visited = Vec::new();
        let mut ae_used = Vec::new();

        // Sender-selected distribution: CC and BCC headers add routing keys.
        let mut keys = vec![routing_key.to_string()];
        for h in ["CC", "BCC"] {
            if let Some(Value::Array(extra)) = headers.get(h) {
                keys.extend(extra.iter().filter_map(|v| v.as_str().map(String::from)));
            }
        }

        if exchange.is_empty() {
            // Default exchange: implicit binding to every queue by name.
            visited.push(String::new());
            for k in &keys {
                if self.queue(k).is_some() {
                    queues.insert(k.clone());
                    hops.push(RouteHop {
                        exchange: String::new(),
                        destination: k.clone(),
                        destination_type: "queue".into(),
                        reason: "default exchange routes to the queue named by the routing key".into(),
                    });
                } else if !self.queues.is_empty() {
                    caveats.push(format!("no known queue named \"{k}\" for the default exchange"));
                }
            }
        } else if let Some(x) = self.exchange(exchange) {
            if x.internal {
                caveats.push(format!(
                    "exchange \"{exchange}\" is internal; clients cannot publish to it directly (the broker will close the channel with 403)"
                ));
            }
            let mut seen = HashSet::new();
            self.route_from(exchange, &keys, headers, &mut seen, &mut visited, &mut hops, &mut queues, &mut ae_used, &mut caveats);
        } else {
            caveats.push(format!(
                "exchange \"{exchange}\" is not in the snapshot; the broker would close the channel with 404"
            ));
        }

        RoutePrediction {
            prediction: true,
            unroutable: queues.is_empty(),
            queues: queues.into_iter().collect(),
            hops,
            exchanges_visited: visited,
            alternate_exchanges_used: ae_used,
            caveats,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn route_from(
        &self,
        exchange: &str,
        keys: &[String],
        headers: &Map<String, Value>,
        seen: &mut HashSet<String>,
        visited: &mut Vec<String>,
        hops: &mut Vec<RouteHop>,
        queues: &mut BTreeSet<String>,
        ae_used: &mut Vec<String>,
        caveats: &mut Vec<String>,
    ) {
        if !seen.insert(exchange.to_string()) {
            return; // cycle; the broker de-duplicates as well
        }
        visited.push(exchange.to_string());
        let Some(x) = self.exchange(exchange) else {
            caveats.push(format!("exchange \"{exchange}\" referenced by a binding is not in the snapshot"));
            return;
        };
        let mut matched_any = false;
        for b in self.bindings.iter().filter(|b| b.source == exchange) {
            let reason = match x.kind.as_str() {
                "direct" => keys
                    .iter()
                    .find(|k| **k == b.routing_key)
                    .map(|k| format!("direct key \"{k}\"")),
                "fanout" => Some("fanout".to_string()),
                "topic" => keys
                    .iter()
                    .find(|k| topic_match(&b.routing_key, k))
                    .map(|k| format!("topic pattern \"{}\" matches \"{k}\"", b.routing_key)),
                "headers" => headers_match(&b.arguments, headers)
                    .then(|| format!("headers {}", describe_x_match(&b.arguments))),
                other => {
                    let msg = format!(
                        "exchange \"{exchange}\" has type \"{other}\" whose routing cannot be simulated; its bindings are listed as possible destinations"
                    );
                    if !caveats.contains(&msg) {
                        caveats.push(msg);
                    }
                    None
                }
            };
            if let Some(reason) = reason {
                matched_any = true;
                hops.push(RouteHop {
                    exchange: exchange.to_string(),
                    destination: b.destination.clone(),
                    destination_type: b.destination_type.clone(),
                    reason,
                });
                if b.destination_type == "exchange" {
                    self.route_from(&b.destination, keys, headers, seen, visited, hops, queues, ae_used, caveats);
                } else {
                    queues.insert(b.destination.clone());
                }
            }
        }
        // Like the broker (rabbit_exchange:process_alternate), the alternate
        // exchange is consulted only when this exchange's own bindings matched
        // nothing. A matching exchange-to-exchange binding counts as a match
        // even if the downstream exchange then routes nowhere.
        if !matched_any {
            if let Some(ae) = x.arguments.get("alternate-exchange").and_then(Value::as_str) {
                ae_used.push(ae.to_string());
                self.route_from(ae, keys, headers, seen, visited, hops, queues, ae_used, caveats);
            } else if x.policy.is_some() {
                caveats.push(format!(
                    "exchange \"{exchange}\" has policy {:?}; an alternate exchange set by policy is not visible in the snapshot",
                    x.policy.as_deref().unwrap_or_default()
                ));
            }
        }
    }

    /// All known resources referenced by bindings but missing from the snapshot,
    /// plus exchanges that have no bindings at all (likely unroutable).
    pub fn lint(&self) -> Vec<String> {
        let mut out = Vec::new();
        let ex: HashSet<_> = self.exchanges.iter().map(|e| e.name.as_str()).collect();
        let qs: HashSet<_> = self.queues.iter().map(|q| q.name.as_str()).collect();
        for b in &self.bindings {
            if !b.source.is_empty() && !ex.contains(b.source.as_str()) {
                out.push(format!("binding source exchange \"{}\" is missing", b.source));
            }
            let ok = match b.destination_type.as_str() {
                "queue" => qs.contains(b.destination.as_str()),
                _ => ex.contains(b.destination.as_str()),
            };
            if !ok {
                out.push(format!("binding destination {} \"{}\" is missing", b.destination_type, b.destination));
            }
        }
        for e in &self.exchanges {
            if e.name.is_empty() || e.name.starts_with("amq.") {
                continue;
            }
            let has = self.bindings.iter().any(|b| b.source == e.name);
            if !has && !e.arguments.contains_key("alternate-exchange") {
                out.push(format!("exchange \"{}\" has no bindings: messages published to it are unroutable", e.name));
            }
        }
        out
    }

    /// Graphviz rendering of the topology.
    pub fn to_dot(&self) -> String {
        let mut s = String::from("digraph rabbitmq {\n  rankdir=LR;\n  node [fontname=\"monospace\"];\n");
        for e in &self.exchanges {
            if e.name.is_empty() {
                continue;
            }
            s.push_str(&format!(
                "  \"x:{}\" [label=\"{}\\n({})\", shape=box{}];\n",
                esc(&e.name),
                esc(&e.name),
                e.kind,
                if e.internal { ", style=dashed" } else { "" }
            ));
            if let Some(ae) = e.arguments.get("alternate-exchange").and_then(Value::as_str) {
                s.push_str(&format!("  \"x:{}\" -> \"x:{}\" [style=dotted, label=\"AE\"];\n", esc(&e.name), esc(ae)));
            }
        }
        for q in &self.queues {
            s.push_str(&format!(
                "  \"q:{}\" [label=\"{}\\n[{}]\", shape=cylinder];\n",
                esc(&q.name),
                esc(&q.name),
                q.kind
            ));
            if let Some(dlx) = q.arguments.get("x-dead-letter-exchange").and_then(Value::as_str) {
                s.push_str(&format!("  \"q:{}\" -> \"x:{}\" [style=dotted, color=red, label=\"DLX\"];\n", esc(&q.name), esc(dlx)));
            }
        }
        for b in &self.bindings {
            if b.source.is_empty() {
                continue;
            }
            let dst = if b.destination_type == "queue" { "q" } else { "x" };
            s.push_str(&format!(
                "  \"x:{}\" -> \"{}:{}\" [label=\"{}\"];\n",
                esc(&b.source),
                dst,
                esc(&b.destination),
                esc(&b.routing_key)
            ));
        }
        s.push_str("}\n");
        s
    }
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// AMQP topic matching. `*` = exactly one word, `#` = zero or more words.
pub fn topic_match(pattern: &str, key: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = if key.is_empty() { vec![] } else { key.split('.').collect() };
    let p: Vec<&str> = if pattern.is_empty() { vec![] } else { p };
    fn rec(p: &[&str], k: &[&str]) -> bool {
        match p.first() {
            None => k.is_empty(),
            Some(&"#") => (0..=k.len()).any(|i| rec(&p[1..], &k[i..])),
            Some(&"*") => !k.is_empty() && rec(&p[1..], &k[1..]),
            Some(w) => k.first() == Some(w) && rec(&p[1..], &k[1..]),
        }
    }
    rec(&p, &k)
}

fn describe_x_match(args: &Map<String, Value>) -> String {
    format!("x-match={}", args.get("x-match").and_then(Value::as_str).unwrap_or("all"))
}

/// Headers exchange matching including `all-with-x` / `any-with-x`.
pub fn headers_match(binding_args: &Map<String, Value>, headers: &Map<String, Value>) -> bool {
    let mode = binding_args.get("x-match").and_then(Value::as_str).unwrap_or("all");
    let include_x = mode.ends_with("-with-x");
    let any = mode.starts_with("any");
    let mut considered = 0;
    let mut matched = 0;
    for (k, v) in binding_args {
        if k == "x-match" || (!include_x && k.starts_with("x-")) {
            continue;
        }
        considered += 1;
        let ok = match headers.get(k) {
            // A binding value of void/null matches presence of the key.
            Some(hv) => v.is_null() || values_equal(v, hv),
            None => false,
        };
        if ok {
            matched += 1;
        }
    }
    if any {
        matched > 0
    } else {
        matched == considered
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TopologyDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

impl TopologyDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Compare two topologies (e.g. two clusters, or a cluster against exported definitions).
pub fn diff(a: &Topology, b: &Topology) -> TopologyDiff {
    let mut d = TopologyDiff::default();
    fn index<T: Serialize>(items: &[T], key: impl Fn(&T) -> String) -> BTreeMap<String, Value> {
        items
            .iter()
            .map(|i| (key(i), serde_json::to_value(i).unwrap_or_default()))
            .collect()
    }
    let sets = [
        (
            index(&a.exchanges, |e| format!("exchange {}", e.name)),
            index(&b.exchanges, |e| format!("exchange {}", e.name)),
        ),
        (
            index(&a.queues, |q| format!("queue {}", q.name)),
            index(&b.queues, |q| format!("queue {}", q.name)),
        ),
        (
            index(&a.bindings, binding_key),
            index(&b.bindings, binding_key),
        ),
    ];
    for (left, right) in sets {
        for (k, v) in &left {
            match right.get(k) {
                None => d.removed.push(k.clone()),
                Some(v2) if strip_volatile(v) != strip_volatile(v2) => d.changed.push(k.clone()),
                _ => {}
            }
        }
        for k in right.keys() {
            if !left.contains_key(k) {
                d.added.push(k.clone());
            }
        }
    }
    d
}

fn binding_key(b: &Binding) -> String {
    format!(
        "binding {} -> {} {} [{}] {}",
        b.source,
        b.destination_type,
        b.destination,
        b.routing_key,
        serde_json::to_string(&b.arguments).unwrap_or_default()
    )
}

fn strip_volatile(v: &Value) -> Value {
    let mut v = v.clone();
    if let Value::Object(m) = &mut v {
        m.remove("vhost");
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn topo() -> Topology {
        serde_json::from_value(json!({
            "exchanges": [
                {"name": "", "type": "direct"},
                {"name": "orders", "type": "topic", "arguments": {"alternate-exchange": "unrouted"}},
                {"name": "unrouted", "type": "fanout"},
                {"name": "fan", "type": "fanout"},
                {"name": "hdr", "type": "headers"},
                {"name": "audit", "type": "direct", "internal": true},
                {"name": "custom", "type": "x-consistent-hash"}
            ],
            "queues": [
                {"name": "eu"}, {"name": "all"}, {"name": "lost"}, {"name": "pdf"}, {"name": "audit-log"}, {"name": "h1"}
            ],
            "bindings": [
                {"source": "orders", "destination": "eu", "destination_type": "queue", "routing_key": "order.eu.*"},
                {"source": "orders", "destination": "all", "destination_type": "queue", "routing_key": "order.#"},
                {"source": "orders", "destination": "audit", "destination_type": "exchange", "routing_key": "order.#"},
                {"source": "audit", "destination": "audit-log", "destination_type": "queue", "routing_key": "order.us.new"},
                {"source": "unrouted", "destination": "lost", "destination_type": "queue", "routing_key": ""},
                {"source": "hdr", "destination": "pdf", "destination_type": "queue", "arguments": {"x-match": "all", "format": "pdf", "type": "report"}},
                {"source": "hdr", "destination": "h1", "destination_type": "queue", "arguments": {"x-match": "any", "format": "pdf", "lang": "en"}},
                {"source": "custom", "destination": "eu", "destination_type": "queue", "routing_key": "10"}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn topic_patterns() {
        assert!(topic_match("a.*", "a.b"));
        assert!(!topic_match("a.*", "a.b.c"));
        assert!(topic_match("a.#", "a"));
        assert!(topic_match("a.#", "a.b.c"));
        assert!(topic_match("#", ""));
        assert!(topic_match("#.c", "a.b.c"));
        assert!(topic_match("*.b.#", "a.b"));
        assert!(!topic_match("*", ""));
        assert!(topic_match("", ""));
        assert!(!topic_match("a", "b"));
    }

    #[test]
    fn topic_and_e2e() {
        let t = topo();
        let p = t.simulate("orders", "order.eu.new", &Map::new());
        assert!(p.prediction);
        assert_eq!(p.queues, vec!["all", "eu"]);
        assert!(!p.unroutable);
        let p = t.simulate("orders", "order.us.new", &Map::new());
        assert_eq!(p.queues, vec!["all", "audit-log"]);
        assert!(p.exchanges_visited.contains(&"audit".to_string()));
    }

    #[test]
    fn alternate_exchange() {
        let t = topo();
        let p = t.simulate("orders", "invoice.x", &Map::new());
        assert_eq!(p.queues, vec!["lost"]);
        assert_eq!(p.alternate_exchanges_used, vec!["unrouted"]);
    }

    #[test]
    fn default_exchange_and_missing() {
        let t = topo();
        assert_eq!(t.simulate("", "eu", &Map::new()).queues, vec!["eu"]);
        let p = t.simulate("", "nope", &Map::new());
        assert!(p.unroutable);
        let p = t.simulate("missing", "k", &Map::new());
        assert!(p.unroutable);
        assert!(p.caveats.iter().any(|c| c.contains("404")));
    }

    #[test]
    fn headers_exchange() {
        let t = topo();
        let h = json!({"format": "pdf", "type": "report"}).as_object().unwrap().clone();
        assert_eq!(t.simulate("hdr", "", &h).queues, vec!["h1", "pdf"]);
        let h = json!({"lang": "en"}).as_object().unwrap().clone();
        assert_eq!(t.simulate("hdr", "", &h).queues, vec!["h1"]);
        let mut args = json!({"x-match": "all-with-x", "x-foo": 1}).as_object().unwrap().clone();
        assert!(!headers_match(&args, &Map::new()));
        args.insert("x-match".into(), json!("all"));
        assert!(headers_match(&args, &Map::new()));
    }

    #[test]
    fn cc_headers_and_internal_and_custom() {
        let t = topo();
        let h = json!({"CC": ["order.eu.x"]}).as_object().unwrap().clone();
        assert!(t.simulate("orders", "zzz", &h).queues.contains(&"eu".to_string()));
        let p = t.simulate("audit", "order.us.new", &Map::new());
        assert!(p.caveats.iter().any(|c| c.contains("internal")));
        let p = t.simulate("custom", "10", &Map::new());
        assert!(p.caveats.iter().any(|c| c.contains("cannot be simulated")));
    }

    #[test]
    fn cycle_safe() {
        let t: Topology = serde_json::from_value(json!({
            "exchanges": [{"name": "a", "type": "fanout"}, {"name": "b", "type": "fanout"}],
            "queues": [{"name": "q"}],
            "bindings": [
                {"source": "a", "destination": "b", "destination_type": "exchange"},
                {"source": "b", "destination": "a", "destination_type": "exchange"},
                {"source": "b", "destination": "q", "destination_type": "queue"}
            ]
        })).unwrap();
        assert_eq!(t.simulate("a", "", &Map::new()).queues, vec!["q"]);
    }

    #[test]
    fn lint_and_dot_and_diff() {
        let mut t = topo();
        t.exchanges.push(serde_json::from_value(json!({"name": "lonely", "type": "direct"})).unwrap());
        let l = t.lint();
        assert!(l.iter().any(|m| m.contains("lonely")));
        let dot = t.to_dot();
        assert!(dot.contains("\"x:orders\" -> \"q:eu\""));
        let mut t2 = t.clone();
        t2.queues.retain(|q| q.name != "eu");
        t2.exchanges[1].kind = "direct".into();
        t2.queues.push(serde_json::from_value(json!({"name": "new"})).unwrap());
        let d = diff(&t, &t2);
        assert_eq!(d.removed, vec!["queue eu"]);
        assert_eq!(d.added, vec!["queue new"]);
        assert_eq!(d.changed, vec!["exchange orders"]);
        assert!(diff(&t, &t).is_empty());
    }
}
