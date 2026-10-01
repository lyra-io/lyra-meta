#!/usr/bin/env bash
# Start a fresh two-shard server. Never run the fixed /catalog keys in a shared namespace.
# Usage: OXIA_BIN=/absolute/path/to/oxia bash scripts/test-lip-0001.sh
set -euo pipefail

: "${OXIA_BIN:?Set OXIA_BIN to an existing Oxia executable}"
test -x "$OXIA_BIN"
cd "$(dirname "$0")/.."

test_port="${LYRA_MVP_PORT:-17648}"
metrics_port="${LYRA_MVP_METRICS_PORT:-18081}"
for port in "$test_port" "$metrics_port"; do
  if [[ ! "$port" =~ ^[0-9]{1,5}$ ]] || ((port < 1024 || port > 65535)); then
    echo "Test ports must be integers between 1024 and 65535" >&2
    exit 1
  fi
  if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "Port $port is already in use; refusing to use an existing server" >&2
    exit 1
  fi
done
if [[ "$test_port" == "$metrics_port" ]]; then
  echo "Public and metrics ports must differ" >&2
  exit 1
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/lyra-meta-mvp.XXXXXX")
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid"
    wait "$server_pid" || true
  fi
  echo "Disposable Oxia data and logs retained at $run_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$OXIA_BIN" --version
"$OXIA_BIN" standalone \
  --data-dir "$run_dir/db" --wal-dir "$run_dir/wal" \
  --public-addr "127.0.0.1:$test_port" --metrics-addr "127.0.0.1:$metrics_port" \
  --shards 2 --log-level warn >"$run_dir/oxia.log" 2>&1 &
server_pid=$!

ready=false
for _ in {1..100}; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    sed -n '1,160p' "$run_dir/oxia.log" >&2
    exit 1
  fi
  if (echo >"/dev/tcp/127.0.0.1/$test_port") 2>/dev/null; then
    ready=true
    break
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "Disposable Oxia did not become ready" >&2
  sed -n '1,160p' "$run_dir/oxia.log" >&2
  exit 1
fi

OXIA_SERVICE_ADDRESS="127.0.0.1:$test_port" LYRA_MVP_DISPOSABLE=1 \
  cargo test --locked --test metadata_contract -- --ignored --exact oxia_implements_the_metadata_contract --nocapture
