# Redis exchange profiling

Runs N real Academy `Echo` agents (Manager, event-loop executor) spread across P launcher processes against a
throwaway Redis. It instruments both the Redis server and the Python clients.

```
profiling/run.sh [--pyspy] --agents N --procs P [--transport unix|tcp] [--socket-timeout default|none|SECS] \
                 [--idle-s 60] [--sizes-kb 0,1,10] [--rounds 3] [--inflight 2000] [--teardown-timeout 120]
uv run python profiling/analyze.py [runs/<ts> ...]     # one row per (run, phase)
```
- `REDIS_HOSTS=127.0.0.1,127.0.0.2` (also pass `--hosts` with the same list) binds Redis on several IPs. Launchers are spread across them, which sidesteps the TCP ephemeral-port cap on a single IP. On macOS, add the aliases first with `sudo ifconfig lo0 alias 127.0.0.N`.
- `--pyspy` writes a CPU profile of the orchestrator and all launchers to `runs/<ts>/profile.json`; open it at speedscope.app. It needs sudo on macOS.

## What is recorded (`runs/<ts>/`)
| file | contents |
|---|---|
| `limits.txt` | fd limit, ephemeral port range, effective Redis `maxclients` |
| `redis_info.jsonl` | 1 Hz `INFO clients/memory/stats/cpu`, tagged with the current phase |
| `client_*.jsonl` | 1 Hz per process (orchestrator and each launcher): event-loop lag, RSS, fds, CPU |
| `summary.json` | per phase (`launch`, `idle`, `echo_<S>kb`, `teardown`): commandstats (counters reset at each phase start), Redis CPU, connections, client memory, slowlog, `LATENCY LATEST`, echo latency/throughput, client aggregates, derived ratios |
| `launcher<i>.log` | launcher stdout/stderr |

## Findings (laptop, M-series 10 cores / 32 GB, academy-py 1.0, redis-py 8.1, Redis 7.0; 2026-11-15)
All runs use P = 8 launchers and the unix socket unless noted.

| N | Redis clients/agent | idle Redis cmds/s | echo/s | Redis CPU in echo | orchestrator CPU in echo | teardown |
|---|---|---|---|---|---|---|
| 1K | 3.0 | 59 | 1.9K | 18% | ~100% | 3.3 s, 397K SCAN |
| 5K | 2.8 | 324 | 2.1K | 20% | 86% | >60 s (killed), 7.1M SCAN, Redis 95% |
| 10K | 2.4 | 896 | 1.85K | 21% | 82% | >30 s (killed), Redis 93% |
| 25K | 2.2 | 2.9K | 1.87K | 24% | 84% | >30 s (killed), Redis 92% |

Bottlenecks for 100K agents on one exchange, most severe first:
1. **Agent `terminate` is O(N) per agent, O(N²) total.** `RedisExchangeTransport.terminate` (`academy/exchange/redis.py:363`) runs `SCAN request:<uid>:*` over the whole keyspace. Shutting down 5K agents already pins Redis for over a minute; 100K will never finish. `discover` (`:175`) and `agent_stats` (`:249`) scan the same way.
2. **redis-py 8's default `socket_timeout=5` breaks blocking receives.**
   - `_recv`'s `BLPOP 0` is cut after 5 s, and the client reconnects and re-issues it. At 10K agents that was 64K reconnects in 35 s idle: 8× the idle commands and 2× the Redis CPU.
   - Under load the retries run out. The `TimeoutError` kills the agent's listener and **the agent shuts down**: 88 `AgentTerminatedError`s at 10K, and echo throughput fell 3× (576/s).
   - Fix: pass `socket_timeout=None` (`--socket-timeout none`) in `RedisExchangeFactory` kwargs.
   - On TCP each reconnect also leaves a socket in TIME_WAIT, so the ephemeral ports run out sooner.
3. **About 2.2–3 Redis connections per agent.** Each transport's pool keeps idle connections open beside the blocked `BLPOP`.
   - 100K agents means about 220K–300K connections into one Redis. That is above macOS's 122,880 per-process fd cap, so on this laptop the stock exchange tops out around 40–50K agents.
   - Over TCP from one host to one Redis IP, `EADDRNOTAVAIL` hits at about 16K connections, which is about 6.7K agents (measured).
   - Spreading across IPs or nodes (`REDIS_HOSTS`, Aurora) removes the port wall but not Redis' fd and connection count.
4. **The single orchestrator client caps throughput at about 1.9K echo/s regardless of N.** It runs at 85–100% of one core, about 0.5 ms of Python per echo, while Redis sits at about 20% CPU. One echo round to 100K agents would take about 55 s. A py-spy run will show where the time goes (likely pydantic serialization or the per-command await chain).
5. **About 13 unpipelined Redis commands per echo** (5 GET, 2 INCRBY, 2 RPUSH, 2 BLPOP, SET, DEL), costing about 0.1 ms of Redis CPU per echo. Redis has roughly 4× headroom before it saturates at this rate; pipelining or Lua would cut it 2–3×.
6. **Idle load is fine once the socket timeout is fixed.** Steady state is one `TIME` + `SET` heartbeat every 30 s per agent, about 6.7K cmds/s at 100K. Redis client memory is about 4 KB per agent idle and about 20 KB under load, roughly 2 GB at 100K.

Not yet run: the multi-IP TCP matrix (needs `sudo ifconfig lo0 alias`), the P = 1/4/8 layout sweep, and a py-spy profile (needs sudo).
