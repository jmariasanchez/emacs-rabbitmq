# Compatibility and qualification

## Reported baseline

As of this documentation snapshot, the supplied baseline is **87 Rust tests and
21 ERT tests passed on macOS against a single RabbitMQ 4.4.0 node**. This is not
a claim that this documentation change reran those suites, and not evidence of
TLS, multi-node cluster, or protocol-conformance qualification.

| Dimension | Current evidence or declaration |
| --- | --- |
| Broker | Single-node RabbitMQ 4.4.0 passing baseline only |
| Operating system | macOS baseline (local), Ubuntu (GitHub Actions `ubuntu-latest`) for CI. No Windows qualification. |
| Rust | Workspace declares `rust-version = "1.85"`, edition 2021. Full locked dependency MSRV is not separately verified here. |
| Emacs | Package declares Emacs 29.1 and Transient 0.4. CI runs bytecompile and unit ERT on Emacs 29.1, 29.4, 30.1 and snapshot. Integration ERT runs on Ubuntu's Emacs 29.3 only. |
| Bridge contract | Protocol version 1 on both Rust and Lisp sides. Prefer matching revisions. |
| AMQP 0-9-1 client | Manifest pins Lapin 4.12.3 |
| AMQP 1.0 client | Manifest pins fe2o3-amqp 0.18.4 |
| Stream client | Manifest pins rabbitmq-stream-client 0.11.0 |
| TLS | AMQP 0-9-1, Streams and Management have configuration paths, unqualified. AMQP 1.0 TLS explicitly unsupported. |
| Cluster behavior | No qualified failover, leader migration, partition, replica or rolling-upgrade matrix |

These versions describe repository metadata, not recommendations to use the
latest available releases. Cargo.lock is the dependency snapshot for reproducible
builds. The macOS Cargo configuration explicitly selects `/usr/bin/cc` and
`/usr/bin/c++` for Apple targets, including native dependency builds.

## Broker capabilities are not qualifications

`rabbitmq-core/src/capabilities.rs` contains version-derived feature flags and
queue argument validation. These are implementation assumptions and UI hints,
not evidence that each accepted broker version or feature combination was tested.
Management probing is best effort, so missing management access can also leave
feature information unavailable. Actual broker responses remain authoritative.

Native Streams and Management require their corresponding broker endpoints.
The local test script enables `rabbitmq_management`, `rabbitmq_stream`, and
`rabbitmq_amqp1_0`. Default fixture ports are AMQP 5672, Management 15672, and
Streams 5552. These describe the fixture, not a required production topology.

## To expand support claims

Record exact broker, plugin, Erlang, Rust, Emacs and OS versions, transport and
authentication settings, node count, commands, skips and failures. Add passing
acceptance evidence for the claimed combinations before changing this document.
See TESTING.md and ROADMAP.md for planned checks.
