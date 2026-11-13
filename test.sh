#!/usr/bin/env bash
# Start a throwaway Redis, run run_agents.py against it, then stop Redis.
# Usage: ./test.sh <num_agents> [redis_port]
set -euo pipefail
AGENTS=${1:?usage: ./test.sh <num_agents> [redis_port]}
PORT=${2:-6390}

redis-server --port "$PORT" --save "" --daemonize yes >/dev/null
trap 'redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1 || true' EXIT
until redis-cli -p "$PORT" ping >/dev/null 2>&1; do sleep 0.1; done

uv run run_agents.py --agents "$AGENTS" --redis-port "$PORT"
