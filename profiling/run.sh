#!/usr/bin/env bash
# Start a throwaway Redis, run the profiling orchestrator against it, then stop Redis.
# Usage: profiling/run.sh [--pyspy] [orchestrator args...]
#   e.g. profiling/run.sh --agents 10000 --procs 8 --transport unix
# Env: REDIS_PORT (default 6391), REDIS_HOSTS (comma list to bind, default 127.0.0.1; pass the same as --hosts)
# --pyspy records a CPU flamegraph of the orchestrator and all launchers (sudo on macOS) to profile.json
# (open at https://www.speedscope.app).
set -euo pipefail
cd "$(dirname "$0")/.."
PYSPY=0
if [[ ${1:-} == --pyspy ]]; then PYSPY=1; shift; fi
RUN=runs/$(date +%Y%m%d-%H%M%S)
PORT=${REDIS_PORT:-6391}
HOSTS=${REDIS_HOSTS:-127.0.0.1}
mkdir -p "$RUN"

# Each agent holds >=1 socket; macOS caps per-process fds at kern.maxfilesperproc (122880).
ulimit -n 120000 2>/dev/null || ulimit -n "$(ulimit -Hn)"
profiling/start_redis.sh "$RUN" "$PORT" ${HOSTS//,/ } | tee "$RUN/limits.txt"
trap 'redis-cli -s "$RUN/redis.sock" shutdown nosave >/dev/null 2>&1 || true' EXIT

PY=$(uv run python -c 'import sys; print(sys.executable)')
CMD=("$PY" profiling/orchestrator.py --run-dir "$RUN" --port "$PORT" --hosts "$HOSTS" "$@")
if (( PYSPY )); then
  SUDO=""
  [[ $(uname) == Darwin ]] && SUDO=sudo
  $SUDO "$(dirname "$PY")/py-spy" record --subprocesses --format speedscope -o "$RUN/profile.json" -- "${CMD[@]}"
  [[ -z $SUDO ]] || sudo chown -R "$USER" "$RUN"
else
  "${CMD[@]}"
fi
