//! Method catalogue: every RPC method with its declared [`Effect`] and how
//! to find the resource it targets. The dispatcher consults this table
//! *before* calling a handler, so a handler cannot run without passing the
//! safety policy.

use rabbitmq_core::safety::{Effect, Resource};
use serde_json::Value;

pub struct Method {
    pub name: &'static str,
    pub effect: Effect,
    /// Resource kind for protected-resource rules and confirmations.
    pub kind: Option<&'static str>,
    /// Parameter holding the resource name.
    pub name_param: Option<&'static str>,
    pub summary: &'static str,
}

const fn m(name: &'static str, effect: Effect, kind: Option<&'static str>, name_param: Option<&'static str>, summary: &'static str) -> Method {
    Method { name, effect, kind, name_param, summary }
}

use Effect::*;

pub const METHODS: &[Method] = &[
    // ---- bridge
    m("bridge.hello", Observe, None, None, "Handshake: protocol version, capabilities"),
    m("bridge.ping", Observe, None, None, "Liveness check"),
    m("bridge.methods", Observe, None, None, "List methods and their effects"),
    m("bridge.audit", Observe, None, None, "Recent audit records for interventions"),
    m("bridge.shutdown", Observe, None, None, "Close all sessions and exit"),
    m("bridge.diagnostics", Observe, None, None, "Redacted diagnostic report"),
    // ---- sessions
    m("session.open", Observe, None, None, "Open a named session for a profile"),
    m("session.close", Observe, None, None, "Close a session and release all resources"),
    m("session.list", Observe, None, None, "List sessions"),
    m("session.info", Observe, None, None, "Session state, epoch, consumers, unsettled counts"),
    // ---- topology (AMQP 0-9-1)
    m("exchange.declare", Write, Some("exchange"), Some("name"), "Declare an exchange"),
    m("exchange.delete", Destructive, Some("exchange"), Some("name"), "Delete an exchange"),
    m("queue.declare", Write, Some("queue"), Some("name"), "Declare a queue (type-validated)"),
    m("queue.inspect", Observe, Some("queue"), Some("name"), "Passive declare: message and consumer counts"),
    m("queue.delete", Destructive, Some("queue"), Some("name"), "Delete a queue"),
    m("queue.purge", Destructive, Some("queue"), Some("name"), "Remove all ready messages from a queue"),
    m("binding.create", Write, Some("binding"), Some("destination"), "Bind queue/exchange to exchange"),
    m("binding.delete", Destructive, Some("binding"), Some("destination"), "Remove a binding (confirm with the destination name)"),
    // ---- messaging (AMQP 0-9-1)
    m("message.publish", Publish, Some("exchange"), None, "Publish messages with confirm classification"),
    m("message.get", Consume, Some("queue"), Some("queue"), "basic.get: removes or unacks messages"),
    m("message.settle", Settle, None, None, "ack / nack / reject a delivery on its own channel"),
    m("message.unsettled", Observe, None, None, "List unsettled delivery ids"),
    m("consumer.start", Consume, Some("queue"), Some("queue"), "Start a consumer (manual ack by default)"),
    m("consumer.cancel", Observe, None, None, "Cancel a consumer"),
    m("consumer.pause", Observe, None, None, "Pause delivery display (bounded buffering)"),
    m("consumer.resume", Observe, None, None, "Resume delivery display"),
    m("consumer.drain", Observe, None, None, "Fetch buffered deliveries"),
    // ---- routing (local prediction)
    m("routing.simulate", Observe, None, None, "Predict routing from known bindings"),
    m("topology.snapshot", Observe, None, None, "Exchanges, queues, bindings for a vhost"),
    m("topology.dot", Observe, None, None, "Graphviz rendering"),
    m("topology.lint", Observe, None, None, "Detect missing references and unroutable exchanges"),
    m("topology.diff", Observe, None, None, "Compare live topology with definitions or another profile"),
    // ---- streams
    m("stream.create", Write, Some("stream"), Some("name"), "Create a stream or super stream"),
    m("stream.delete", Destructive, Some("stream"), Some("name"), "Delete a stream or super stream"),
    m("stream.publish", Publish, Some("stream"), Some("stream"), "Publish to a stream"),
    m("stream.read", Observe, Some("stream"), Some("stream"), "Non-destructive bounded replay"),
    m("stream.subscribe", Observe, Some("stream"), Some("stream"), "Live subscription (non-destructive)"),
    m("stream.unsubscribe", Observe, None, None, "Close a stream subscription"),
    m("stream.pause", Observe, None, None, "Pause a subscription's display"),
    m("stream.resume", Observe, None, None, "Resume a subscription's display"),
    m("stream.metadata", Observe, None, None, "Leader and replicas"),
    m("stream.partitions", Observe, None, None, "Super stream partitions"),
    m("stream.route", Observe, None, None, "Partitions for a routing key"),
    m("stream.query_offset", Observe, None, None, "Stored consumer offset"),
    m("stream.store_offset", Write, Some("stream"), Some("stream"), "Store a consumer offset"),
    m("stream.producer_sequence", Observe, None, None, "Last publishing id for a named producer"),
    m("stream.subscriptions", Observe, None, None, "List subscriptions"),
    // ---- AMQP 1.0
    m("amqp10.sender.open", Observe, None, None, "Attach a sender link"),
    m("amqp10.send", Publish, Some("link"), Some("link"), "Send on a sender link"),
    m("amqp10.sender.close", Observe, None, None, "Detach a sender link"),
    m("amqp10.receiver.open", Consume, Some("link"), None, "Attach a receiver link (consumes on settle)"),
    m("amqp10.settle", Settle, None, None, "accepted / rejected / released / modified"),
    m("amqp10.credit", Observe, None, None, "Grant link credit"),
    m("amqp10.drain", Observe, None, None, "Drain link credit"),
    m("amqp10.receiver.close", Observe, None, None, "Detach a receiver link"),
    m("amqp10.links", Observe, None, None, "List links"),
    // ---- management (read)
    m("mgmt.overview", Observe, None, None, "Cluster overview"),
    m("mgmt.nodes", Observe, None, None, "Nodes and resource usage"),
    m("mgmt.node", Observe, None, None, "One node with memory breakdown"),
    m("mgmt.health", Observe, None, None, "Health checks"),
    m("mgmt.feature_flags", Observe, None, None, "Feature flags"),
    m("mgmt.deprecated_features", Observe, None, None, "Deprecated features in use"),
    m("mgmt.whoami", Observe, None, None, "Current user and tags"),
    m("mgmt.vhosts", Observe, None, None, "Virtual hosts"),
    m("mgmt.queues", Observe, None, None, "Queues (paged, filtered)"),
    m("mgmt.queue", Observe, None, None, "One queue with stats"),
    m("mgmt.queue_bindings", Observe, None, None, "Bindings of a queue"),
    m("mgmt.exchanges", Observe, None, None, "Exchanges (paged, filtered)"),
    m("mgmt.exchange", Observe, None, None, "One exchange"),
    m("mgmt.bindings", Observe, None, None, "All bindings in a vhost"),
    m("mgmt.connections", Observe, None, None, "Client connections"),
    m("mgmt.connection", Observe, None, None, "One connection"),
    m("mgmt.channels", Observe, None, None, "Channels"),
    m("mgmt.consumers", Observe, None, None, "Consumers"),
    m("mgmt.users", Observe, None, None, "Users (no password hashes)"),
    m("mgmt.permissions", Observe, None, None, "Permissions"),
    m("mgmt.user_permissions", Observe, None, None, "Permissions of a user"),
    m("mgmt.topic_permissions", Observe, None, None, "Topic permissions"),
    m("mgmt.policies", Observe, None, None, "Policies"),
    m("mgmt.operator_policies", Observe, None, None, "Operator policies"),
    m("mgmt.parameters", Observe, None, None, "Runtime parameters"),
    m("mgmt.global_parameters", Observe, None, None, "Global parameters"),
    m("mgmt.vhost_limits", Observe, None, None, "Virtual host limits"),
    m("mgmt.user_limits", Observe, None, None, "User limits"),
    m("mgmt.definitions.export", Observe, None, None, "Export definitions"),
    m("mgmt.stream_connections", Observe, None, None, "Stream protocol connections"),
    m("mgmt.stream_publishers", Observe, None, None, "Stream publishers"),
    m("mgmt.stream_consumers", Observe, None, None, "Stream consumers"),
    m("mgmt.dead_letter_report", Observe, None, None, "Queues with dead-letter configuration and their targets"),
    // ---- management (write)
    m("mgmt.vhost.create", Write, Some("vhost"), Some("name"), "Create a virtual host"),
    m("mgmt.vhost.delete", Destructive, Some("vhost"), Some("name"), "Delete a virtual host and everything in it"),
    m("mgmt.user.put", Write, Some("user"), Some("name"), "Create or update a user"),
    m("mgmt.user.delete", Destructive, Some("user"), Some("name"), "Delete a user"),
    m("mgmt.permissions.set", Write, Some("user"), Some("user"), "Set vhost permissions"),
    m("mgmt.permissions.clear", Destructive, Some("user"), Some("user"), "Revoke vhost permissions"),
    m("mgmt.topic_permissions.set", Write, Some("user"), Some("user"), "Set topic permissions"),
    m("mgmt.topic_permissions.clear", Destructive, Some("user"), Some("user"), "Revoke topic permissions"),
    m("mgmt.policy.put", Write, Some("policy"), Some("name"), "Create or update a policy"),
    m("mgmt.policy.delete", Destructive, Some("policy"), Some("name"), "Delete a policy"),
    m("mgmt.operator_policy.put", Write, Some("policy"), Some("name"), "Create or update an operator policy"),
    m("mgmt.operator_policy.delete", Destructive, Some("policy"), Some("name"), "Delete an operator policy"),
    m("mgmt.vhost_limit.set", Write, Some("vhost"), Some("vhost"), "Set a vhost limit"),
    m("mgmt.vhost_limit.clear", Write, Some("vhost"), Some("vhost"), "Clear a vhost limit"),
    m("mgmt.connection.close", Destructive, Some("connection"), Some("name"), "Force-close a client connection"),
    m("mgmt.definitions.import", Destructive, Some("definitions"), None, "Import definitions (may overwrite)"),
];

pub fn find(name: &str) -> Option<&'static Method> {
    METHODS.iter().find(|m| m.name == name)
}

impl Method {
    pub fn resource(&self, params: &Value, default_vhost: &str) -> Option<Resource> {
        let kind = self.kind?;
        let name = match self.name_param {
            Some(p) => params.get(p).and_then(Value::as_str).unwrap_or(""),
            None => "",
        };
        let vhost = params.get("vhost").and_then(Value::as_str).unwrap_or(default_vhost);
        Some(Resource::new(kind, Some(vhost), name))
    }
}

pub fn effect_name(e: Effect) -> &'static str {
    match e {
        Observe => "observe",
        Consume => "consume",
        Settle => "settle",
        Publish => "publish",
        Write => "write",
        Destructive => "destructive",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_names_and_destructive_have_names() {
        let mut seen = std::collections::HashSet::new();
        for m in METHODS {
            assert!(seen.insert(m.name), "duplicate {}", m.name);
            if m.effect == Destructive && m.name != "mgmt.definitions.import" {
                assert!(m.name_param.is_some(), "{} must name its target for typed confirmation", m.name);
            }
        }
    }

    #[test]
    fn get_and_consume_are_not_observations() {
        for n in ["message.get", "consumer.start", "amqp10.receiver.open"] {
            assert_eq!(find(n).unwrap().effect, Consume, "{n}");
        }
        for n in ["stream.read", "stream.subscribe", "routing.simulate", "queue.inspect"] {
            assert_eq!(find(n).unwrap().effect, Observe, "{n}");
        }
    }
}
