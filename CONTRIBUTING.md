# Contributing

This is experimental software, not production ready. Changes must preserve the
distinction between code that exists, paths covered by tests, and environments
that are actually qualified. The roadmap is unfinished.

## Work from the checkout

Inspect `Cargo.toml`, `Cargo.lock`, the relevant crate, its tests, and the Lisp
caller before changing an operation. Do not assume source comments describe
complete behavior. Use the real scripts in TESTING.md, not stale `make` commands
mentioned in older comments. No canonical remote URL is supplied here.

Keep patches focused and do not revert unrelated work. For changes touching RPC:

1. Update the method catalogue's effect and target metadata.
2. Check the handler and all indirect broker resources for safety enforcement.
3. Keep transport-specific semantics and ambiguous outcomes explicit.
4. Update frontend call sites and error/notification handling.
5. Consider a protocol version change for breaking method/parameter/result changes.
6. Add tests and update the relevant root documentation.

## Validation

```sh
cargo build --locked
cargo test --workspace --lib --locked
tests/ert/run.sh '(not (tag :integration))'
```

For broker-facing changes, also run the isolated workflow from TESTING.md,
including `RMQ_REQUIRE_BROKER=1 cargo test --workspace --locked` and full ERT.
Report exact commands, versions, successes, failures and skips. ERT integration
skips and Rust early-return tests must not be hidden behind an aggregate pass.
Never use a shared broker or valuable messages as fixtures.

Cover both success and failure behavior. In particular, test protected targets,
read-only sessions, wrong delivery ownership, uncertain publishes, slow readers,
resource limits, cancellation and cleanup when relevant. The planned checks in
TESTING.md are future acceptance criteria, not a list of already passing tests.

The supplied historical baseline is 84 Rust and 17 ERT tests passed on a single
RabbitMQ 4.4.0 node on macOS. Do not imply TLS, cluster, or conformance coverage
from that result, and do not claim a fresh run unless you actually performed it.

## Documentation and diagnostics

Keep README setup commands aligned with actual binaries and scripts. Prefer
local source paths over speculative repository URLs. Feature tables must include
limits and point to concrete existing tests. Planned features must remain
clearly labeled. Review logs for credentials, payloads and private topology
before attaching them to any report. SECURITY.md describes the current gaps.

## Licensing and provenance

The workspace manifest declares `MIT OR Apache-2.0`; Lisp headers declare
`GPL-3.0-or-later`. There is no root license text in the inspected checkout.
This document does not create, reconcile, or replace those declarations and
does not establish a unified project license. Preserve existing notices, track
the origin of contributions, and obtain maintainer clarification before making
redistribution or relicensing assumptions. Do not add a license or repository
identity based on a guess.
