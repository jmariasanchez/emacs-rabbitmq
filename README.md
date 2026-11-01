# rabbitmq.el

An experimental Emacs interface to RabbitMQ, backed by a Rust stdio bridge.
The bridge owns AMQP 0-9-1, AMQP 1.0, native Stream protocol, and Management
HTTP API connections. Emacs provides profiles, tables, message buffers,
producer/consumer commands, topology tools, and administration menus.

**Not production ready. The roadmap is not complete.** The reported baseline
is **87 Rust tests and 21 ERT tests passed against one RabbitMQ 4.4.0 node on
macOS**. This is not TLS, cluster, or protocol-conformance qualification.
Protected-resource checks are incomplete, delivery notifications can be lost,
consumer memory bounds are incomplete, AMQP 1.0 TLS is unsupported, and
cancellation semantics remain uncertain. See SECURITY.md and TESTING.md.

## Build and load

The manifests declare Rust 1.85 and edition 2021. This is not a verified minimum
toolchain for all locked dependencies. The Emacs package declares Emacs 29.1
and Transient 0.4. Build from this checkout, with Cargo and a native C toolchain:

```sh
cargo build --release --locked
./target/release/rabbitmq-bridge --version
./target/release/rabbitmq-bridge --methods
```

Add the checkout's `lisp/` directory to Emacs and point at the built executable:

```elisp
(add-to-list 'load-path "/absolute/path/to/emacs-rabbitmq/lisp")
(setq rabbitmq-bridge-program
      "/absolute/path/to/emacs-rabbitmq/target/release/rabbitmq-bridge")
(require 'rabbitmq)
(setq rabbitmq-profiles
      '(("local" :host "localhost"
         :management-url "http://localhost:15672"
         :username "guest" :read-only t)))
```

Use `M-x rabbitmq-connect`, then `M-x rabbitmq` for the Transient menu.
The example deliberately uses a read-only profile. The built-in profile default
is **not** read-only. Queue get/consume changes broker state, even when presented
as inspection. Use a disposable broker and deliberate consent for experiments.
Passwords can be resolved through `auth-source` using host, username, and an
AMQP port identifier. The local guest fallback is only for local experiments.

## Local tests

With RabbitMQ installed and the default test ports free:

```sh
scripts/test-broker.sh start
cargo build --locked
RMQ_REQUIRE_BROKER=1 cargo test --workspace --locked
tests/ert/run.sh
scripts/test-broker.sh stop
```

Read TESTING.md before running these commands: tests mutate broker resources,
and ERT can skip integrations when the broker is absent. A Makefile provides build, test, bytecompile, ert, and check targets.
The test broker script is not a hardened deployment recipe.

## Documentation

| Document | Purpose |
| --- | --- |
| VISION.md | Intended experience and design principles |
| SCOPE.md | Implemented boundaries and non-goals |
| PARITY.md | Feature inventory without a parity certification |
| ARCHITECTURE.md | Modules, ownership, and data flow |
| RPC.md | Current stdio protocol and method families |
| SECURITY.md | Threat model, safeguards, and known gaps |
| COMPATIBILITY.md | Declared requirements versus tested environment |
| TESTING.md | Actual commands, existing tests, planned acceptance checks |
| ROADMAP.md | Unfinished work and exit criteria |
| CONTRIBUTING.md | Change and evidence expectations |

No canonical repository URL or unified license grant is established here.
Cargo metadata and Lisp headers contain different license declarations. See
CONTRIBUTING.md before redistribution.
