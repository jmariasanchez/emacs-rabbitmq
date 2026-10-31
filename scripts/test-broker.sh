#!/usr/bin/env bash
# Start/stop an isolated RabbitMQ node for integration tests.
# Usage: scripts/test-broker.sh start|stop|status
# Data lives under .test-broker/ (git-ignored). Never points at a shared broker.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${RMQ_TEST_BASE:-$ROOT/.test-broker}"
SBIN="${RABBITMQ_SBIN:-$(dirname "$(command -v rabbitmq-server 2>/dev/null || echo /opt/homebrew/opt/rabbitmq/sbin/rabbitmq-server)")}"
export RABBITMQ_NODENAME="${RABBITMQ_NODENAME:-rabbitmqel-test@localhost}"
export RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
export RABBITMQ_NODE_PORT="${RABBITMQ_NODE_PORT:-5672}"
export RABBITMQ_MNESIA_BASE="$BASE/mnesia"
export RABBITMQ_LOG_BASE="$BASE/log"
export RABBITMQ_ENABLED_PLUGINS_FILE="$BASE/enabled_plugins"
export RABBITMQ_CONFIG_FILE="$BASE/rabbitmq"
export RABBITMQ_PID_FILE="$BASE/pid"
mkdir -p "$BASE/mnesia" "$BASE/log"
echo '[rabbitmq_management,rabbitmq_stream,rabbitmq_amqp1_0].' > "$RABBITMQ_ENABLED_PLUGINS_FILE"
cat > "$BASE/rabbitmq.conf" <<CONF
loopback_users = none
management.tcp.port = ${RMQ_MGMT_PORT:-15672}
stream.listeners.tcp.1 = ${RMQ_STREAM_PORT:-5552}
stream.advertised_host = localhost
CONF
case "${1:-start}" in
  start)
    "$SBIN/rabbitmq-server" -detached
    for _ in $(seq 1 60); do
      if "$SBIN/rabbitmqctl" -q await_startup >/dev/null 2>&1; then
        "$SBIN/rabbitmqctl" -q enable_feature_flag all >/dev/null 2>&1 || true
        echo "broker ready ($RABBITMQ_NODENAME)"; exit 0
      fi
      sleep 1
    done
    echo "broker failed to start; see $RABBITMQ_LOG_BASE" >&2; exit 1 ;;
  stop) "$SBIN/rabbitmqctl" stop || true ;;
  status) "$SBIN/rabbitmqctl" status ;;
  ctl) shift; "$SBIN/rabbitmqctl" "$@" ;;
  *) echo "usage: $0 start|stop|status|ctl ..." >&2; exit 2 ;;
esac
