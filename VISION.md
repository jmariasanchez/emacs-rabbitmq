# Vision

Build a keyboard-driven RabbitMQ development and administration environment
where observation is easy, interventions are explicit, and uncertainty is
visible rather than converted into apparent success.

The implementation already connects an Emacs frontend to protocol-specific
Rust adapters. It is experimental, not production ready, and does not complete
the roadmap. The 84 Rust / 17 ERT passing baseline on single-node RabbitMQ 4.4.0
on macOS is a starting point, not a qualification claim.

## Design principles

- Keep protocol distinctions visible. Ordinary queue consumption changes queue
  state. Stream replay reads an append-only history without acknowledging or
  deleting it. AMQP 1.0 dispositions are not AMQP 0-9-1 delivery tags.
- Keep credentials and broker connections out of UI rendering logic. Emacs
  resolves credentials, and the bridge performs broker operations.
- Treat routing simulation as a prediction from available topology, not proof
  of the broker's eventual routing or plugin behavior.
- Explain denied operations, stale information, partial results, and uncertain
  publish outcomes. Never automatically replay an ambiguous intervention.
- Aim for responsive UI and bounded resource use. Current buffering and output
  paths do not yet provide an end-to-end memory bound or lossless delivery.
- Prefer inspectable method effects, explicit consent, and typed destructive
  confirmation. These are accident-prevention mechanisms, not authorization.

## Intended outcome, not current guarantee

A developer should be able to inspect topology, publish controlled test data,
follow deliveries, settle them deliberately, replay streams, and inspect
administration state without leaving Emacs. An operator should eventually have
credible safety and failure evidence before using the tool on valuable systems.
That latter goal requires the acceptance work in ROADMAP.md, especially complete
resource resolution, backpressure, cancellation cleanup, TLS, and cluster tests.

The project does not aim to replace broker authorization, production monitoring,
backup procedures, or an independent protocol conformance suite.
