# Feature inventory and parity boundaries

This is an implementation inventory, not a certification of parity with the
RabbitMQ Management UI, CLI tools, other clients, or every protocol feature.
The project is experimental, not production ready, and its roadmap is unfinished.

| Surface | Present implementation | Important boundary |
| --- | --- | --- |
| Queue topology | Classic/quorum/stream type model, declarations and bindings | Argument validation is local and selected, not exhaustive broker equivalence |
| AMQP 0-9-1 publishing | Confirms, mandatory-return classification, transaction path | Confirmed publication is not proof of downstream processing |
| AMQP 0-9-1 receiving | Get, consumer lifecycle, prefetch, delivery ownership and settlement | Notification loss and incomplete memory bounds remain |
| AMQP 1.0 | Sender/receiver actor tasks, dispositions, credit, link listing | TLS unsupported, no conformance qualification, no dedicated full AMQP 1.0 UI |
| Native Streams | Replay, subscription, filtering, offsets, producer IDs, super streams | No cluster failover qualification or universal loss recovery |
| Management reads | Typed and generic endpoint handling, paging, sampled results | Endpoint availability and permissions can vary, not every broker endpoint is exposed |
| Management writes | Selected vhost/user/permission/policy/limit/definition operations | Not a complete administration replacement, indirect protection checks incomplete |
| Routing tools | Direct/topic/fanout/headers and exchange graph prediction, lint, diff, DOT | Snapshot-based prediction cannot certify live routing or arbitrary plugins |
| Safety UX | Read-only profiles, consumption consent, typed confirmation, audit | Local policy is not broker authorization or a durable compliance ledger |
| Emacs experience | Tables, producer/consumer/message buffers, topology and admin commands | RPC and adapter presence does not imply an equally complete interactive workflow |

## Evidence levels

1. **Implemented** means a code path exists in the checkout.
2. **Existing test** means a named test exercises a particular path. See TESTING.md.
3. **Reported passing baseline** means 84 Rust and 17 ERT tests passed on a single
   RabbitMQ 4.4.0 node on macOS.
4. **Qualified** would require an explicit environment, failure model, acceptance
   criteria, and retained evidence. TLS, clusters, and conformance are not qualified.

Do not promote level 1 or 2 to level 4. In particular, tests of local safety
predicates do not prove every RPC resolves the actual protected resource, a
bounded UI buffer does not bound receiver memory, and one cancellation response
does not demonstrate rollback or cleanup of every adapter task.
