# Contributing

This is experimental software, not production ready. Changes must preserve the
distinction between code that exists, paths covered by tests, and environments
that are actually qualified. The roadmap is unfinished.

## Work from the checkout

Inspect `Cargo.toml`, `Cargo.lock`, the relevant crate, its tests, and the Lisp
caller before changing an operation. Do not assume source comments describe
complete behavior. Use the real scripts in TESTING.md, not stale `make` commands
mentioned in older comments. The canonical repository is
<https://github.com/jmariasanchez/emacs-rabbitmq>. Report issues and open pull
requests there.

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

The supplied historical baseline is 87 Rust and 21 ERT tests passed on a single
RabbitMQ 4.4.0 node on macOS. Do not imply TLS, cluster, or conformance coverage
from that result, and do not claim a fresh run unless you actually performed it.

## Documentation and diagnostics

Keep README setup commands aligned with actual binaries and scripts. Prefer
local source paths over speculative repository URLs. Feature tables must include
limits and point to concrete existing tests. Planned features must remain
clearly labeled. Review logs for credentials, payloads and private topology
before attaching them to any report. SECURITY.md describes the current gaps.

## Licensing and provenance

The project is split-licensed by component. The Emacs Lisp package (`lisp/`,
`tests/ert/`) is GPL-3.0-or-later (COPYING). The Rust workspace (`crates/`) is
`MIT OR Apache-2.0` (LICENSE-MIT, LICENSE-APACHE). The bridge is a separate
program that talks to Emacs over stdio. Contributions are accepted under the
license of the component they modify. Keep SPDX headers on new Lisp files and
`license.workspace = true` on new crates.
