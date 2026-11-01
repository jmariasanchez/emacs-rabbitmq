# Roadmap

**The roadmap is not complete.** The project is experimental and not production
ready. Existing functionality and the 87 Rust / 21 ERT passing baseline on
single-node RabbitMQ 4.4.0 on macOS do not close the work below.

## Implemented foundation

The checkout has four broker adapters, a versioned stdio bridge, an Emacs UI,
selected safety and audit mechanisms, topology tools, and unit/live/ERT tests.
PARITY.md inventories the surface. TESTING.md names the evidence. This foundation
does not establish TLS, cluster, or conformance qualification.

## Priority 1: close known safety and reliability gaps

- [ ] Complete protected-resource resolution for every intervention, including
  indirect queue/exchange targets, AMQP 1.0 links, settlement, bindings, and
  definitions import. Acceptance: denied operations leave broker state unchanged
  across an explicit method-to-resource test inventory.
- [ ] Replace misleading notification recovery assumptions with a defined
  delivery handoff/backpressure or replay design. Acceptance: slow-reader and
  saturation tests account for every delivery and expose any unrecoverable loss.
- [ ] Bound consumer resource use end to end, including bytes, unsettled maps,
  batches and response queues. Acceptance: sustained-load measurements show
  enforced ceilings without silent settlement loss.
- [ ] Define cancellation and timeout semantics per adapter. Acceptance: race
  tests demonstrate cleanup and correctly mark uncertain broker effects.
- [ ] Implement AMQP 1.0 TLS without plaintext fallback. Acceptance: positive
  certificate tests and negative trust/hostname/client-auth tests pass.

## Priority 2: qualify supported environments

- [ ] Run and retain transport-specific TLS and authentication suites.
- [ ] Add multi-node broker tests for failure, leader movement and recovery.
- [ ] Add protocol conformance and malformed-input tests independent of the
  implementation's own assumptions.
- [ ] Establish actual Rust dependency MSRV and an Emacs/OS/broker test matrix.
- [ ] Verify lifecycle cleanup under bridge crash, EOF, cancellation and load.
- [ ] Review test-broker listener exposure and cleanup target validation.

## Priority 3: improve user and maintainer experience

- [ ] Fill documented gaps between adapter/RPC methods and interactive UI flows.
- [ ] Validate audit completeness, retention and redaction with adversarial data.
- [ ] Add reproducible CI evidence and release/package checks.
- [x] Establish repository identity and license texts (split GPL-3.0-or-later
  for Lisp, MIT OR Apache-2.0 for Rust).

## Release gate

Before making a production-readiness claim, close the known critical gaps,
publish the scope of passed acceptance checks and supported environments, retain
failure-test evidence, and document remaining limitations. A test count alone
is not a release gate. No completion date or comprehensive parity promise is
made by this roadmap.
