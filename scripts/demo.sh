#!/usr/bin/env bash
# Starts a three-node cluster on this machine, writes a key, kills the leader
# with SIGKILL, and shows that the key is still readable from the survivors.
set -euo pipefail
cd "$(dirname "$0")/.."

go build -o bin/raftkv ./cmd/raftkv
DATA="$(mktemp -d)"
CLUSTER="n1=127.0.0.1:7001/127.0.0.1:8001,n2=127.0.0.1:7002/127.0.0.1:8002,n3=127.0.0.1:7003/127.0.0.1:8003"

declare -A PID
start() { ./bin/raftkv -id "$1" -cluster "$CLUSTER" -data "$DATA/$1" 2>"$DATA/$1.log" & PID[$1]=$!; }
cleanup() { kill "${PID[@]}" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT

leader() {
  for port in 8001 8002 8003; do
    if curl -s --max-time 1 "127.0.0.1:$port/status" | grep -q '"state":"leader"'; then
      curl -s "127.0.0.1:$port/status" | sed -E 's/.*"id":"([^"]+)".*/\1/'
      return
    fi
  done
}
wait_leader() { for _ in $(seq 50); do L="$(leader)"; [ -n "$L" ] && { echo "$L"; return; }; sleep 0.2; done; echo "no leader" >&2; exit 1; }

for n in n1 n2 n3; do start "$n"; done
L="$(wait_leader)"
echo "leader elected: $L"

curl -s -L -X PUT --data "hello raft" 127.0.0.1:8001/kv/greeting
echo "wrote greeting; read from every node (followers redirect to the leader):"
for port in 8001 8002 8003; do echo "  :$port -> $(curl -s -L 127.0.0.1:$port/kv/greeting)"; done

echo "killing leader $L with SIGKILL"
kill -9 "${PID[$L]}"; unset "PID[$L]"
sleep 0.2
L2="$(wait_leader)"
echo "new leader elected: $L2"
for port in 8001 8002 8003; do
  v="$(curl -s -L --max-time 2 127.0.0.1:$port/kv/greeting || true)"
  [ -n "$v" ] && echo "  :$port -> $v"
done

echo "restarting $L; it rejoins and catches up from the new leader"
start "$L"
sleep 1.5
for port in 8001 8002 8003; do echo "  :$port $(curl -s 127.0.0.1:$port/status)"; done
