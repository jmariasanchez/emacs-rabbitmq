# Current RPC contract

The bridge speaks a JSON-RPC 2.0-shaped protocol over stdio, protocol version **1**.
This describes the implementation, not full JSON-RPC or AMQP conformance.
The client and bridge should be built from the same revision while experimental.

## Transport and envelopes

Frames are `Content-Length: N\r\n\r\n` followed by exactly N UTF-8 JSON bytes.
N is the byte count, not the character count. The Rust reader accepts bodies up
to 64 MiB. Stdout is reserved for protocol output during normal bridge operation.
Logs go to stderr or `--log-file PATH`. `--version` and `--methods` are separate
human-readable CLI modes, not RPC sessions.

Requests use integer or string IDs, and responses echo IDs. Request parameters
are objects. Notifications have no ID. These are JSON bodies, not complete frames:

```json
{"jsonrpc":"2.0","id":1,"method":"bridge.hello","params":{}}
{"jsonrpc":"2.0","id":2,"method":"session.open","params":{"session":"local","profile":{"name":"local","host":"localhost","username":"guest","password":"guest","read_only":true}}}
{"jsonrpc":"2.0","id":3,"method":"queue.inspect","params":{"session":"local","name":"example"}}
{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":3}}
```

The credentials above are for an isolated local test only. Do not log profile
request bodies. `bridge.hello` reports program and protocol versions, capability
labels, and selected limits. Capability labels describe compiled code, not
successful TLS negotiation or live feature qualification.

## Discovering methods and shapes

```sh
./target/debug/rabbitmq-bridge --methods
```

`bridge.methods` returns names, effects, summaries and target kinds at runtime.
The registry is `crates/rabbitmq-bridge/src/methods.rs`. Parameter/result shapes
are implemented in `handlers.rs` and the adapter structs, not a generated schema.

| Family | Examples and semantics |
| --- | --- |
| `bridge.*` | hello, ping, methods, audit, diagnostics, shutdown |
| `session.*` | open, close, list, info |
| `exchange.*`, `queue.*`, `binding.*` | AMQP 0-9-1 topology, inspect, delete, purge |
| `message.*`, `consumer.*` | publish/get/settle/unsettled, start/cancel/pause/resume/drain |
| `routing.*`, `topology.*` | simulate, snapshot, dot, lint, diff |
| `stream.*` | create/delete/publish/read, subscriptions, offsets, metadata, partitions and routes |
| `amqp10.*` | sender/receiver links, send/settle, credit/drain, links |
| `mgmt.*` | Selected Management API reads and writes, definitions import/export |

Session operations normally require `params.session`. Profiles and JSON keys
use snake_case. Emacs converts profile plist kebab-case keys to snake_case.
AMQP 0-9-1 operations are bound to the connection's vhost and reject a conflicting
vhost override. Management operations can select a vhost separately.

## Effects, errors and outcomes

Effects are `observe`, `consume`, `settle`, `publish`, `write`, `destructive`.
Read-only profiles reject interventions. Consumption needs `consent: true`.
Destruction needs a matching `confirm` target name, with special handling for
definitions import. These checks do not make protected-resource coverage complete.

Responses contain `result` or `error`. Errors include a numeric code and message,
and domain errors include structured `data` with a symbolic code, message,
retryability, ambiguity and optional hint/detail. The exact mapping is in
`crates/rabbitmq-core/src/error.rs`. Unknown methods use `-32601`, parse errors
use `-32700`, and cancellation uses `-32800`.

Publish responses distinguish outcomes such as confirmed, returned, failed and
uncertain rather than reducing every send to a success boolean. An ambiguous
outcome must be reconciled before retrying, to avoid duplicate interventions.

## Notifications and cancellation limitations

Emitted events include `$/session`, `$/deliveries`, `$/consumer`, `$/returned`,
`$/event`, and `$/audit`. Payloads carry session/consumer identifiers where
applicable. A declared `$/progress` name is not a guarantee that every long
operation emits progress. Delivery batches are not a durable event stream.

Notifications can be dropped when the 4096-entry output queue fills. A warning
and drop count do not reconstruct lost messages. `consumer.drain` retrieves only
what remains buffered, not every batch already removed for notification delivery.

`$/cancelRequest` aborts the registered request task and responds to its original
ID if it is still in flight. Intervention cancellation is marked ambiguous.
Late cancellation can be a no-op. Broker effects and spawned adapter work may
continue, and cleanup semantics are not fully qualified. The existing cancellation
test covers an idle stream read, not every intervention race. A local Emacs
timeout is likewise not proof that the broker did nothing.

The parser's permissive envelope handling, framing recovery, duplicate IDs,
backpressure and cancellation races need further acceptance tests. Do not infer
batch-request support or exhaustive protocol validation from the JSON-RPC label.
