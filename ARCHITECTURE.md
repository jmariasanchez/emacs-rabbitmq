# Architecture

The application consists of an Emacs frontend and one Rust bridge process.
This is an experimental architecture with unresolved reliability and safety
gaps, not a production-qualified design.

```text
Emacs commands, profiles, buffers
  -> rabbitmq-rpc.el / jsonrpc.el
  -> Content-Length framed JSON on bridge stdin/stdout
  -> dispatcher -> method effect / safety check -> handler
  -> AMQP 0-9-1 | AMQP 1.0 | Streams | Management HTTP
  <- responses and asynchronous notifications
```

## Module ownership

| Module | Responsibility |
| --- | --- |
| `lisp/rabbitmq-core.el` | Profile conversion, auth-source resolution, session UI state, consent and confirmation helpers |
| `lisp/rabbitmq-rpc.el` | Bridge discovery/process lifecycle, version handshake, asynchronous calls and notification handlers |
| Other `lisp/rabbitmq-*.el` | Feature-specific commands, tables, buffers and presentation |
| `rabbitmq-core` | Profiles, errors, safety predicates, messages, buffers, redaction, topology and capability model |
| `rabbitmq-protocol` | Wire envelopes, request IDs, protocol version and framing |
| `rabbitmq-bridge` | Method registry, dispatch, session registry, adapters, audit and stdout writer |
| `rabbitmq-amqp091` | Lapin connection/channel lifecycle, confirms, consumers, delivery ownership and settlement |
| `rabbitmq-amqp10` | fe2o3-amqp links and command-driven sender/receiver tasks |
| `rabbitmq-streams` | Native Stream client, producers, replay and subscriptions |
| `rabbitmq-management` | Reqwest Management HTTP client, endpoint models and topology conversion |
| `rabbitmq-testkit` | Disposable resource naming, broker probe and independent HTTP assertions |

## Session and operation lifecycle

`session.open` accepts a profile and optional session ID. Management version
probing is best effort. AMQP 0-9-1 is opened eagerly unless `lazy` is requested.
Some connection failures become session warnings when management is configured,
so successful session creation does not prove every transport is connected.
Adapters are held in per-session `OnceCell`s and initialized as needed.

The dispatcher looks up each method's declared effect, finds the session and
resource metadata, applies the profile safety policy, calls the handler, and
records intervention outcomes. Resource metadata is incomplete for some methods.
It is not a complete authorization boundary.

Requests run in Tokio tasks, with an in-flight registry capped at 256. Cancellation
aborts the registered request task. Adapter actors and broker operations can
outlive that task, so cancellation does not imply rollback or complete cleanup.
Session close and process EOF attempt to close adapters. The process has finite
shutdown timeouts, not a guarantee that all remote cleanup has completed.

## Ownership and buffering

AMQP 0-9-1 tracks delivery ownership by connection epoch and channel, rejects
stale settlement, and has reconnect logic that does not restore consumers
automatically. Streams use offsets instead of queue delivery tags. AMQP 1.0
receiver tasks retain disposition state and communicate through command queues.

One writer owns stdout. Responses use an unbounded channel, and notifications
use a 4096-entry channel with drop accounting. The in-flight cap does not prove
that the response backlog is bounded when stdout is stalled. Bounded message
buffers and payload previews exist, but they are not an end-to-end bound on
unsettled deliveries, receiver batches, internal client queues, or total bytes.

Some delivery batches leave adapter buffers before notification enqueue succeeds.
Consequently, `consumer.drain` cannot be assumed to recover every lost batch.
The drop warning is diagnostic, not reliable replay. ROADMAP.md defines the
missing reliability acceptance work.
