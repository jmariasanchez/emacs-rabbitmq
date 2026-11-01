#!/usr/bin/env bash
# Run the ERT suite in batch mode. Integration tests need the bridge built
# (cargo build) and the test broker running (scripts/test-broker.sh start).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SELECTOR="${1:-t}"
exec emacs --batch -Q -L "$ROOT/lisp" -L "$ROOT/tests/ert" \
  --eval "(setq rabbitmq-bridge-program (or (getenv \"RABBITMQ_BRIDGE\") \"$ROOT/target/debug/rabbitmq-bridge\"))" \
  -l rabbitmq-test \
  --eval "(ert-run-tests-batch-and-exit '$SELECTOR)"
