# Security and operational safety

**Experimental, not production ready.** Use disposable data and narrowly scoped
broker credentials. The reported single-node RabbitMQ 4.4.0 macOS baseline
(84 Rust and 17 ERT tests) is not a security audit, TLS qualification, cluster
qualification, or conformance assessment.

## Trust boundaries

Emacs and the bridge run as the local user. Emacs resolves passwords and sends
them through the local process pipe. The bridge can connect to configured
endpoints, read configured certificate/key files, and write requested logs.
The RPC interface is not an authenticated network service or a sandbox for
untrusted callers. Broker permissions remain the real authorization boundary.

Do not treat `observe` as literally no state change anywhere: connections,
receiver links and protocol bookkeeping can be created. The effect classification
primarily distinguishes application-level interventions from inspection.

## Existing safeguards

- A bridge-side method catalogue assigns effects before handler execution.
- Read-only profiles deny interventions. The default profile is not read-only.
- Ordinary queue consumption requires explicit consent, and destructive actions
  require typed confirmation in the UI plus a matching RPC confirmation value.
- Protected rules match resource kind/vhost/name globs where targets are supplied.
- Secret wrappers mask serialized passwords and debug output. Redaction helpers
  sanitize selected keys, credential URLs, errors and diagnostic structures.
- Intervention audit records are kept in a bounded in-memory log, with optional
  JSONL file output through `--audit-log PATH`.
- Selected payload previews, display buffers, request counts and notification
  queues have limits.

These are partial safeguards, not a complete safety proof. Redaction is not a
guarantee for arbitrary message bodies, custom headers, broker error strings,
or Emacs process/debug logs. Review diagnostics before sharing them. Audit files
need local permissions, retention management and review. The audit path is not
a tamper-resistant or guaranteed durable compliance ledger.

## Known gaps and safe operating response

| Gap | Consequence and response |
| --- | --- |
| Protected-resource checks incomplete | Some method targets are empty, indirect, link-based or span multiple resources. Do not rely on glob rules to protect production resources. Use broker permissions and disposable vhosts. |
| Notification delivery loss | Slow readers can lose batches after adapter buffers were drained. Do not assume `consumer.drain` recovers everything or that absence in the UI means absence at the broker. |
| Consumer memory bounds incomplete | Unsettled maps, actor batches and internal queues are not all globally byte-bounded. Avoid sustained unattended load and monitor process memory. |
| AMQP 1.0 TLS unsupported | The adapter rejects TLS profiles. Do not downgrade a real deployment to plaintext to accommodate it. |
| Cancellation semantics uncertain | Aborting an RPC task is not rollback. Broker effects and adapter work may survive. Inspect resulting broker state before retrying. |

TLS configuration paths exist for AMQP 0-9-1, Streams and Management, but none
is qualified by the baseline. AMQP 0-9-1 rejects `tls.verify=false`; other adapters
have their own behavior. Keep verification enabled, configure the intended CA,
and test endpoint-specific certificate and authentication failures before use.

## Test broker and cleanup cautions

`scripts/test-broker.sh` writes local state under `.test-broker/` by default,
uses guest credentials, enables test plugins, and writes `loopback_users = none`.
Do not assume every plugin listener is loopback-only merely because the script
sets the AMQP node IP. Check listeners and firewall rules. Do not expose this
fixture to an untrusted network.

`scripts/clean-test-resources.sh` deletes matching `rmqel-test.*` queues,
exchanges, users and policies. Its localhost URL check is a simple string-prefix
guard, not robust host validation. Inspect the URL and broker identity yourself.
Never point tests or cleanup at a shared or production broker.

## Reporting sensitive problems

No dedicated private reporting address or disclosure channel is established by
this document. Use a maintainer-approved private channel if available. Do not
post credentials, message bodies, private topology, or an exploitable production
configuration in a public issue. Include a sanitized reproduction against a
disposable broker and identify the affected revision and transport.
