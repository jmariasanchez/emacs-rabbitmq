# Testing and evidence

## Baseline and limits

The integrated run on November 1, 2026 passed **87 Rust tests and 21 ERT tests
against one RabbitMQ 4.4.0 node on macOS**, with `RMQ_REQUIRE_BROKER=1` for Rust.
Rust formatting and Emacs bytecompilation also passed. Clippy completed with
warnings. The isolated test broker was stopped after validation. The implementation is experimental and not production
ready. TLS, cluster behavior, and protocol conformance are **not qualified**.
The mappings below are selected existing tests, not an exhaustive matrix.

## Commands that exist in this checkout

Use an isolated broker and disposable data. `make check` runs build, Rust tests,
bytecompilation, and non-integration ERT tests. The GitHub Actions workflow adds
a RabbitMQ integration lane, but has not been executed on hosted infrastructure.

```sh
# Build and run pure library tests without requiring a broker.
cargo build --locked
cargo test --workspace --lib --locked

# Run ERT tests that are not tagged integration.
tests/ert/run.sh '(not (tag :integration))'

# Full local workflow, with RabbitMQ installed and default ports available.
scripts/test-broker.sh start
scripts/test-broker.sh status
RMQ_REQUIRE_BROKER=1 cargo test --workspace --locked
tests/ert/run.sh
scripts/test-broker.sh stop
```

`tests/ert/run.sh` accepts an ERT selector as its first argument. It starts Emacs
with `--batch -Q`, adds `lisp/` and `tests/ert/`, and defaults to
`target/debug/rabbitmq-bridge`. Set `RABBITMQ_BRIDGE` to an alternate binary path.
The integration tests probe `127.0.0.1:5672` and use the local guest profile and
Management port 15672. Check the summary for skips. `RMQ_REQUIRE_BROKER=1` does
not make ERT's skip logic fail.

Rust live tests call the testkit broker probe. Without `RMQ_REQUIRE_BROKER=1`,
an unreachable broker can make tests return early and still appear passed.
The flag makes that TCP probe fail hard, but it does not certify all endpoints.
Testkit recognizes `RMQ_TEST_HOST`, `RMQ_TEST_AMQP_PORT`, `RMQ_TEST_STREAM_PORT`
and `RMQ_TEST_MGMT_URL`. Its admin helper uses guest credentials. Do not assume
cleanup-script credential variables configure the Rust or ERT tests.

The broker script supports `start`, `stop`, `status`, and `ctl ...` and uses
`.test-broker/` by default. Optional configuration includes `RABBITMQ_SBIN`,
`RMQ_TEST_BASE`, `RABBITMQ_NODENAME`, `RABBITMQ_NODE_PORT`, `RMQ_MGMT_PORT` and
`RMQ_STREAM_PORT`. Changing broker ports also requires matching clients, and
the current ERT fixtures are fixed to default endpoints.

After inspecting the target and leftovers, this optional command deletes
`rmqel-test.*` queues/exchanges/users/policies from the configured local fixture:

```sh
scripts/clean-test-resources.sh
```

It is destructive, not a dry run, and not a general cleanup tool. See SECURITY.md.

## Feature-to-existing-test mapping

Paths below are relative to the checkout. A test name records the specific
coverage available, not proof of all modes or every failure path.

| Feature | Existing test source and examples |
| --- | --- |
| Safety predicates | `crates/rabbitmq-core/src/safety.rs`: `read_only_blocks_interventions`, `consume_requires_consent`, `destructive_requires_matching_confirm`, `protected_rules` |
| Secrets and errors | `crates/rabbitmq-core/src/profile.rs`: `secret_never_serialized`; `crates/rabbitmq-core/src/redact.rs`: `uri_userinfo`, `json_keys`; `crates/rabbitmq-core/src/error.rs`: `messages_are_redacted` |
| Payload and routing model | `crates/rabbitmq-core/src/message.rs`: `payload_roundtrip`, `preview_truncation_respects_utf8`; `crates/rabbitmq-core/src/topology.rs`: `topic_and_e2e`, `headers_exchange`, `cycle_safe`, `lint_and_dot_and_diff` |
| AMQP 0-9-1 publish/settle | `crates/rabbitmq-amqp091/tests/live.rs`: `publish_confirm_and_get_with_manual_ack`, `mandatory_unroutable_is_returned_not_confirmed`, `transactions_commit` |
| AMQP 0-9-1 lifecycle/failure | Same file: `consumer_manual_ack_prefetch_and_cancel`, `queue_deleted_while_consuming_invalidates_deliveries`, `connection_loss_invalidates_unacked_and_reconnects_without_resubscribing`, `auth_failure_is_classified_and_redacted` |
| AMQP 1.0 outcomes/credit | `crates/rabbitmq-amqp10/tests/live.rs`: `send_receive_settle_outcomes_and_credit`, `unroutable_exchange_publish_is_released_not_accepted`, `attaching_to_missing_queue_fails_cleanly`, `reject_dead_letters` |
| Streams | `crates/rabbitmq-streams/tests/live.rs`: `replay_is_non_destructive_and_repeatable`, `named_producer_deduplicates`, `filtering_and_offset_tracking`, `live_subscription_delivers_batches`, `super_stream_partitions_and_routes` |
| Management | `crates/rabbitmq-management/tests/live.rs`: `overview_nodes_health`, `queue_exchange_binding_lifecycle_and_topology`, `paging_and_filtering`, `definitions_export_and_diff`, `users_permissions_and_access_refused`, `policies_roundtrip` |
| RPC workflow and safety | `crates/rabbitmq-bridge/tests/rpc.rs`: `handshake_and_unknown_method`, `full_workflow_with_safety_gates_and_audit`, `read_only_profile_blocks_interventions_but_allows_observation` |
| RPC lifecycle | Same file: `consumer_notifications_and_stream_replay_over_rpc`, `cancellation_reports_cancelled`, `stdin_close_releases_broker_connections` |
| Emacs profiles and consent | `tests/ert/rabbitmq-test.el`: `rabbitmq-test-profile-json-defaults`, `rabbitmq-test-auth-source-password`, `rabbitmq-test-confirm-destructive` |
| Emacs display and producers | Same file: `rabbitmq-test-producer-parse`, `rabbitmq-test-hexdump-and-decode`, `rabbitmq-test-messages-buffer-bounded` |
| Emacs/bridge integration | Same file: `rabbitmq-test-integration-handshake`, `rabbitmq-test-integration-publish-get-settle`, `rabbitmq-test-integration-async-consumer-notifications`, `rabbitmq-test-integration-stream-replay`, `rabbitmq-test-integration-routing-and-topology`, `rabbitmq-test-integration-queues-buffer`, `rabbitmq-test-integration-read-only-profile`, `rabbitmq-test-integration-bridge-crash-is-detected` |

The existing cancellation test cancels an idle stream read. It does not prove
intervention rollback, actor cleanup, or all timeout races. The bounded Emacs
message-buffer test does not establish a process-wide memory bound. Local
`protected_rules` tests do not prove complete RPC resource resolution.

## Planned acceptance checks, not completed tests

| Risk | Required acceptance evidence |
| --- | --- |
| Protected resources | Enumerate intervention methods and direct/indirect targets, deny protected targets across publish/link/settlement/binding/import paths, verify broker state unchanged after denial |
| Notification loss | Stall stdout while producing deliveries above queue capacity, account for each delivery and settlement token, demonstrate documented recovery or explicit terminal loss without a false recovery promise |
| Memory bounds | Sustained slow readers, paused consumers, auto-credit and unacknowledged traffic with large payloads, assert limits across batches/unsettled maps/client queues and measure RSS over time |
| Cancellation | Cancel before/during/after broker acceptance for each adapter, check ambiguity, resource cleanup, late responses, surviving actor tasks, and safe reconciliation without blind retry |
| TLS/authentication | Trusted/untrusted CA, hostname mismatch, expired certificate, client identity, failed auth and verification settings on each supported transport; AMQP 1.0 must first gain TLS support |
| Clusters | Multi-node leader loss, replica migration, partitions and recovery, reconnect and replay/offset behavior, duplicate/loss accounting |
| Protocol conformance | Independent protocol assertions, malformed frames, split/coalesced frames, wrong versions, duplicate IDs, oversized headers/bodies, shutdown races and unsupported message forms |
| Broader compatibility | Repeat the real build and integration workflow for each claimed OS/toolchain/Emacs/broker combination, record exact versions and skips |

For every new claim, retain commands, revision, environment, counts and logs with
secrets removed. A green run with skipped or early-return integration paths must
not be presented as full broker coverage.
