#!/usr/bin/env bash
# Start a throwaway, profiling-friendly redis-server in the background.
# Usage: start_redis.sh <run_dir> <port> [bind_hosts...]
# Listens on TCP <port> on each bind host and on the unix socket <run_dir>/redis.sock.
set -euo pipefail
RUN=${1:?run_dir}; PORT=${2:?port}; shift 2
HOSTS=${*:-127.0.0.1}

ulimit -n "$(ulimit -Hn)" 2>/dev/null || ulimit -n 200000 2>/dev/null || true
echo "redis fd limit: $(ulimit -n)"
if [[ $(uname) == Darwin ]]; then
  echo "ephemeral ports: $(sysctl -n net.inet.ip.portrange.first)-$(sysctl -n net.inet.ip.portrange.last)"
else
  echo "ephemeral ports: $(tr '\t' - < /proc/sys/net/ipv4/ip_local_port_range)"
fi

# shellcheck disable=SC2086
redis-server --port "$PORT" --bind $HOSTS --unixsocket "$RUN/redis.sock" --unixsocketperm 700 \
  --maxclients 150000 --tcp-backlog 65535 --save "" --appendonly no \
  --latency-monitor-threshold 5 --slowlog-log-slower-than 1000 --slowlog-max-len 1024 \
  --daemonize yes --pidfile "$RUN/redis.pid" --logfile "$RUN/redis.log"
until redis-cli -s "$RUN/redis.sock" ping >/dev/null 2>&1; do sleep 0.1; done
echo "redis maxclients: $(redis-cli -s "$RUN/redis.sock" config get maxclients | tail -1)"
