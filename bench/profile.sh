#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
LABEL="${1:-after}"
TMP="$(mktemp -d)"
PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  docker rm -f minikv-profile-client >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT

cd "$ROOT"
"$PYTHON" -m cProfile -o "$TMP/minikv.prof" -m minikv \
  --appendfsync no --appendfilename "$TMP/appendonly.aof" >"$TMP/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do
  if "$PYTHON" -c 'import socket; s=socket.socket(); s.settimeout(.1); s.connect(("127.0.0.1",6380)); s.close()' 2>/dev/null; then break; fi
  sleep .05
done
docker run -d --rm --name minikv-profile-client redis:7-alpine sleep 300 >/dev/null
docker exec minikv-profile-client redis-benchmark -h host.docker.internal -p 6380 \
  -t set,get,incr,lpush -n 50000 -c 50 -P 16 -q > "$TMP/benchmark.txt"
kill -TERM "$PID"
wait "$PID"
"$PYTHON" -c 'import pstats,sys; pstats.Stats(sys.argv[1]).strip_dirs().sort_stats("cumtime").print_stats(30)' \
  "$TMP/minikv.prof" > "$ROOT/bench/profile-$LABEL.txt"
cp "$TMP/benchmark.txt" "$ROOT/bench/profile-workload-$LABEL.txt"
cat "$ROOT/bench/profile-$LABEL.txt"
