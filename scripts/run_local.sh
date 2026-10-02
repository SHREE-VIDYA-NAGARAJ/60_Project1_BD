#!/usr/bin/env bash
# Run a whole cluster on this machine without Docker:
#     ./scripts/run_local.sh [NUM_DATANODES]      (default 3)
# Web UI: http://localhost:8000      Stop everything with Ctrl-C.
set -euo pipefail
N="${1:-3}"
cd "$(dirname "$0")/.."
export NAMENODE_HOST=127.0.0.1 NAMENODE_PORT=5000 ADVERTISE_HOST=127.0.0.1
export NAMENODE_DATA_DIR=./local_data/namenode
PIDS=()
cleanup() { echo; echo "stopping cluster..."; kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

python3 namenode.py & PIDS+=($!)
sleep 1
for i in $(seq 0 $((N-1))); do
  DATANODE_ID="datanode$i" DATANODE_PORT=$((6000+i)) DATA_DIR="./local_data/datanode$i" python3 datanode.py &
  PIDS+=($!)
done
python3 client.py & PIDS+=($!)
echo "cluster up: 1 NameNode, $N DataNodes, web UI on http://localhost:8000"
wait
