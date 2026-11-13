# academy-scale — Phase 1 plan: (1) agent-count perf test, (2) vLLM + pi-agents demo

## Context
Goal: Demonstrate that Academy can drive ~100k-agent science campaigns on HPC (Aurora). This plan covers only the first two pieces; hierarchy/2-machine/hosted-exchange work comes after.

Decisions already made:
- Target: **Aurora (PBS, Intel XPU)**; everything also runs on the laptop at small N (same code, different config).
- (1) = **active messaging load**, **star topology** (workers talk only to an orchestrator), **stock Academy + Redis exchange**, N swept to failure, plus a **raw redis-py baseline**.
- (2) = **vLLM everywhere with HF weights**, model **Llama-3-8B-Instruct** (laptop has only Ollama GGUF, so HF weights must be downloaded; vLLM-CPU on macOS is smoke-test only).

Key facts from exploration:
- `~/src/academy/academy/exchange/redis.py:130` — each mailbox opens its own `redis.asyncio.Redis`; `_recv` does `BLPOP` (`:220`) → **≥1 Redis connection per agent**. Redis default `maxclients=10000` + fd ulimits are the expected first wall.
- `academy/manager.py:307` — Manager's default executor is `event_loop`, so thousands of agents can share one process via `Manager.launch()` (`:584`).
- `RedisExchangeFactory(hostname, port, **redis_kwargs)` (`redis.py:444`).
- academy-pi `PiCodingAgent` (github academy-agents/academy-pi, `src/academy_pi/agent.py`) wraps `pi --mode rpc`; actions `prompt`, `prompt_background`, `wait_until_idle`, `get_last_reply`.
- pi reads custom OpenAI-compatible endpoints from `<agent-dir>/models.json` (`"api": "openai-completions"`, `baseUrl`), agent dir set via `PI_CODING_AGENT_DIR` — so pointing pi at vLLM needs **no code change to PiCodingAgent**: launcher writes a models.json and sets the env var before spawning agents (child `pi` processes inherit it).

## Repo layout (new, `git init` in `~/src/academy-scale`)
```
pyproject.toml          # uv; academy-py (editable path ~/src/academy), redis, pandas/pyarrow;
                        # academy-pi (git dep); extra [vllm]
src/academy_scale/
  common.py             # load TOML config, run-dir creation, agent-id registry (JSON files in run dir)
  perf/
    worker.py           # WorkerAgent(Agent): @action echo(payload: bytes) -> bytes
    orchestrator.py     # drive load: per-worker handle, target total rate R, payload size S,
                        #   duration T; records (send_ts, recv_ts) per request
    launcher.py         # per-node process: K WorkerAgents in one Manager (event_loop executor),
                        #   writes their AgentIds to run_dir/agents/<host>-<pid>.json, waits
    redis_baseline.py   # same pattern w/o Academy: N asyncio clients BLPOP, driver LPUSH/reply
    redis_probe.py      # samples Redis INFO (connected_clients, used_memory, cpu) to CSV
  llm/
    vllm_agent.py       # VLLMServiceAgent(Agent): on_startup spawns `vllm serve <model>`
                        #   subprocess, polls /v1/models until ready; actions endpoint(), health();
                        #   on_shutdown terminates
    pi_swarm.py         # driver: launch VLLMServiceAgent → get endpoint → write models.json →
                        #   launch M PiCodingAgents → fan out prompts → collect replies + timings
experiments/
  01_agent_scale/  configs/{laptop,aurora_*}.toml, pbs/run_sweep.pbs, analyze.py
  02_vllm_pi/      configs/{laptop,aurora}.toml, pbs/run_demo.pbs
tests/                  # pytest, local redis-server, small N
```

## Piece 1 — agent-count perf test
Run shape (Aurora): node 0 = `redis-server` (maxclients 200000, `ulimit -n` raised, `--save ""`) + orchestrator + probe; nodes 1..P = `mpiexec` of `launcher.py`, K agents/process, several processes/node.
Steps:
1. Launchers start, register K workers each, write IDs; orchestrator waits until N IDs present (records startup time).
2. Orchestrator issues `echo` round-robin at rate R for T seconds (asyncio, bounded in-flight), records latency.
3. Teardown via Manager context exit (records shutdown time).
Sweep N = 1k, 5k, 10k, 25k, 50k, 100k (vary P and K); also sweep R at fixed N. Stop condition per point: errors/timeouts > 1% or startup exceeds timeout → that's the ceiling.
Outputs per run (`run_dir/`): `latency.parquet`, `redis_info.csv`, `summary.json` (N, P, K, R, S, p50/p99 latency, achieved tput, startup/teardown s, errors). `analyze.py` → plots of latency & throughput vs N, Academy vs raw baseline.
Baseline: `redis_baseline.py` with identical N/R/S on identical node layout.

## Piece 2 — vLLM service agent + pi-agents
1. `VLLMServiceAgent(model, port, extra_args)` launches vLLM on the node it runs on (Aurora: XPU build of vLLM in a module/conda env on a GPU node; laptop: vLLM CPU build), returns `http://<host>:<port>/v1`.
2. `pi_swarm.py`: Manager on the Redis exchange (reuse piece-1 config loader); launch the vLLM agent; once `endpoint()` returns, write `run_dir/pi-agent-dir/models.json` with provider `vllm` → that baseUrl, model id = served model; set `PI_CODING_AGENT_DIR`; launch M `PiCodingAgent(workdir=run_dir/ws/<i>, provider="vllm", model=...)`; send each a small task (e.g. "write a file answer.txt with the sum of 1..100"), gather replies, verify files, record per-agent latency.
3. Demo scales M = 1, 8, 32 on one Aurora node (vLLM on 1+ GPU tiles) — larger M is phase 3.
Model weights: download `meta-llama/Meta-Llama-3-8B-Instruct` (HF gated — needs your HF token) to laptop and to `/flare/<project>/models/` on Aurora.

## Verification
- `uv run pytest`: WorkerAgent echo round-trip; orchestrator with 100 agents on local `redis-server` produces summary.json with 0 errors; redis_baseline runs; VLLMServiceAgent tested with a fake server script (readiness polling, shutdown).
- Laptop: `experiments/01_agent_scale` laptop config at N=1k–5k → plots generated. Piece 2 laptop smoke: vLLM CPU + M=2 pi agents complete the task.
- Aurora: submit `run_sweep.pbs` small (2 nodes, N=10k) before the full sweep; then `run_demo.pbs` with M=8.

## Open items needed at execution time (not blocking plan)
- Aurora project/allocation + queue for PBS scripts; HF token for Llama weights; whether a vLLM-XPU env already exists on Aurora (else build it — likely the riskiest step in piece 2).
