# Scope of the current implementation

Status: experimental and not production ready. Implementation presence is not
equivalent to qualification, UI completeness, or completion of the roadmap.

## Present in source

| Area | Current boundary |
| --- | --- |
| Emacs | Profiles/auth-source, sessions, Transient dispatch, queue and cluster tables, message rendering, producer and consumer buffers, streams, topology, administration, monitoring and event views |
| AMQP 0-9-1 | Exchanges, queues and bindings, queue-type validation, confirms and transactions, get/consume, manual settlement, prefetch, cancellation, connection-loss handling |
| AMQP 1.0 | Sender/receiver links, address construction, send outcomes, receiver credit and drain, accepted/rejected/released/modified settlement |
| Streams | Stream and super-stream creation/deletion, publish, offset/timestamp replay, subscriptions, filtering, named producer IDs, consumer offsets, metadata, partitions and routes |
| Management | Overview, nodes, health, queue/exchange listings, connections, users, permissions, policies, limits, definitions and stream metadata endpoints |
| Local tools | Topology snapshot conversion, routing prediction, DOT output, lint and diff, structured errors, redaction and intervention audit |
| RPC | Content-Length framing, handshake, method catalogue, concurrent requests, notifications, cancellation, diagnostics and shutdown |

The definitive exposed method list is `crates/rabbitmq-bridge/src/methods.rs`.
Handler shapes live in `handlers.rs` and adapter request types. Not every Rust
adapter feature has a dedicated interactive Emacs command. PARITY.md describes
these distinctions without claiming equivalence to other clients or tools.

## Explicit exclusions and limits

- No production-readiness, complete RabbitMQ feature coverage, or exhaustive
  protocol conformance claim.
- No verified multi-node failover, partition recovery, rolling upgrade, or
  cross-version/platform support matrix.
- No AMQP 1.0 TLS implementation. Other TLS paths exist but are not qualified.
- No reliable delivery journal, durable notification replay, or proven global
  memory bound. Pausing display is not necessarily stopping broker delivery.
- No rollback guarantee on cancellation, timeout, disconnect, or process death.
- Protected rules do not yet resolve every affected broker resource, including
  indirect targets and operations spanning multiple resources.
- No automatic replay guarantee for consumers or ambiguous publishes after a
  reconnect. No distributed transaction or exactly-once processing guarantee.
- No full-message retrieval promise for truncated previews merely because a
  profile or source comment describes on-demand access.

## Evidence boundary

The supplied baseline is 84 Rust tests and 17 ERT tests passed on macOS against
a single RabbitMQ 4.4.0 node. The existing tests cover selected successful and
failure paths. TESTING.md maps features to named tests and separately lists
planned acceptance checks. It is deliberately not an exhaustive matrix.
