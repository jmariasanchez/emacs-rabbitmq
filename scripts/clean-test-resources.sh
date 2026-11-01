#!/usr/bin/env bash
# Remove leftover `rmqel-test.*` queues, exchanges, users, and policies from
# the isolated test broker. Refuses to run against anything but localhost.
set -euo pipefail
MGMT="${RMQ_TEST_MGMT_URL:-http://localhost:15672}"
case "$MGMT" in http://localhost*|http://127.0.0.1*) ;; *) echo "refusing: $MGMT is not local" >&2; exit 1;; esac
A=(-s -u "${RMQ_TEST_USER:-guest}:${RMQ_TEST_PASS:-guest}")
enc() { python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$1"; }
names() { curl "${A[@]}" "$MGMT/api/$1" | python3 -c 'import json,sys;[print(x["name"]) for x in json.load(sys.stdin) if x["name"].startswith("rmqel-test.")]'; }
n=0
for q in $(names queues/%2F); do curl "${A[@]}" -X DELETE "$MGMT/api/queues/%2F/$(enc "$q")" >/dev/null; n=$((n+1)); done
for x in $(names exchanges/%2F); do curl "${A[@]}" -X DELETE "$MGMT/api/exchanges/%2F/$(enc "$x")" >/dev/null; n=$((n+1)); done
for u in $(names users); do curl "${A[@]}" -X DELETE "$MGMT/api/users/$(enc "$u")" >/dev/null; n=$((n+1)); done
for p in $(names policies/%2F); do curl "${A[@]}" -X DELETE "$MGMT/api/policies/%2F/$(enc "$p")" >/dev/null; n=$((n+1)); done
echo "removed $n test resources"
