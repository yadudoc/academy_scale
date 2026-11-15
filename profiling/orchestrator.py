"""Profile an Academy Redis exchange with N real Echo agents spread over P launcher processes.

Phases (each bracketed by Redis probe snapshots):
  launch   - P launchers start N/P agents each (event-loop executor); ends when all agent IDs are registered
  idle     - --idle-s seconds of background traffic only (heartbeats etc.)
  echo_<S> - (--pattern echo) --rounds rounds of echo(S KB) to every agent, at most --inflight outstanding
  broadcast_<S> - (--pattern broadcast) every agent echoes S KB off all N agents: N^2 requests + N^2 replies
  teardown - launchers shut their agents down and exit (killed after --teardown-timeout)
Writes <run_dir>/summary.json with Redis-side and client-side metrics per phase.
"""

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import redis.asyncio as redis
from academy.handle import Handle
from academy.identifier import AgentId

from launcher import make_factory, sample_metrics, socket_timeout_kwargs
from redis_probe import RedisProbe

HERE = Path(__file__).parent


def spawn_launchers(args: argparse.Namespace, run: Path) -> list[subprocess.Popen]:
    hosts = args.hosts.split(",")
    per, extra = divmod(args.agents, args.procs)
    procs = []
    for i in range(args.procs):
        cmd = [sys.executable, str(HERE / "launcher.py"), "--run-dir", str(run), "--index", str(i),
               "--agents", str(per + (i < extra)), "--transport", args.transport,
               "--host", hosts[i % len(hosts)], "--port", str(args.port), "--batch", str(args.launch_batch),
               "--socket-timeout", args.socket_timeout]
        procs.append(subprocess.Popen(cmd, stdout=open(run / f"launcher{i}.log", "w"), stderr=subprocess.STDOUT))
    return procs


async def wait_for_ids(run: Path, procs: list[subprocess.Popen], timeout: float) -> list[str]:
    deadline = time.monotonic() + timeout
    while len(files := list((run / "agents").glob("*.json"))) < len(procs):
        if dead := [i for i, p in enumerate(procs) if p.poll() is not None]:
            raise RuntimeError(f"launcher(s) {dead} exited during launch; see {run}/launcher<i>.log")
        if time.monotonic() > deadline:
            raise TimeoutError(f"only {len(files)}/{len(procs)} launchers ready after {timeout}s")
        await asyncio.sleep(0.5)
    return [uid for f in files for uid in json.loads(f.read_text())]


async def echo_round(handles: list[Handle], payload: bytes, inflight: int, timeout: float) -> dict:
    sem = asyncio.Semaphore(inflight)

    async def one(h: Handle) -> float:
        async with sem:
            start = time.perf_counter()
            await asyncio.wait_for(h.echo(payload), timeout)
            return time.perf_counter() - start

    start = time.perf_counter()
    results = await asyncio.gather(*(one(h) for h in handles), return_exceptions=True)
    wall = time.perf_counter() - start
    lat = sorted(r for r in results if isinstance(r, float))
    errors = [r for r in results if not isinstance(r, float)]
    return {"wall_s": wall, "ok": len(lat), "errors": len(errors),
            "error_types": sorted({type(e).__name__ for e in errors}),
            "p50_s": lat[len(lat) // 2] if lat else None, "p99_s": lat[int(len(lat) * 0.99)] if lat else None}


def client_metrics(run: Path, t_start: float, t_end: float) -> dict:
    """Aggregate client_*.jsonl samples within a phase window (+1 sample, so sub-second phases get one): per-process maxima, summed across launchers."""
    out = {}
    for f in sorted(run.glob("client_*.jsonl")):
        rows = [r for line in f.read_text().splitlines() if t_start <= (r := json.loads(line))["t"] <= t_end + 1.1]
        if rows:
            out[f.stem.removeprefix("client_")] = {
                "lag_max_s": max(r["lag_max_s"] for r in rows), "rss_max": max(r["rss"] for r in rows),
                "fds_max": max(r["fds"] for r in rows), "cpu_s": rows[-1]["cpu_s"] - rows[0]["cpu_s"]}
    launchers = [v for k, v in out.items() if k.startswith("launcher")]
    return {"per_process": out,
            "launchers_rss_total": sum(v["rss_max"] for v in launchers),
            "launchers_fds_total": sum(v["fds_max"] for v in launchers),
            "launchers_lag_max_s": max((v["lag_max_s"] for v in launchers), default=None)}


async def main(args: argparse.Namespace) -> None:
    run = Path(args.run_dir)
    (run / "agents").mkdir(parents=True, exist_ok=True)
    host = args.hosts.split(",")[0]
    sock = str(run / "redis.sock")
    redis_kwargs = {"unix_socket_path": sock} if args.transport == "unix" else {}
    probe = RedisProbe(redis.Redis(host=host, port=args.port, **redis_kwargs), run)
    probe.start()
    stop_metrics = asyncio.Event()
    metrics = asyncio.create_task(sample_metrics(run / "client_orchestrator.jsonl", stop_metrics))
    config = vars(args)
    phases = []

    async def end_phase(**extra):
        phase = await probe.end()
        phase.update(extra)
        phases.append(phase)
        print(json.dumps({k: phase[k] for k in ("phase", "wall_s", "redis_cpu_s", "commands_total",
                                                 "connected_clients")} | extra), flush=True)

    # Orchestrator traffic goes through one client; its pool grows to the number of concurrent commands.
    factory = make_factory(args.transport, host, args.port, sock, max_connections=args.inflight + 64,
                           **socket_timeout_kwargs(args.socket_timeout))
    procs = []
    try:
        await probe.begin("launch")
        procs = spawn_launchers(args, run)
        uids = await wait_for_ids(run, procs, args.launch_timeout)
        await end_phase(agents=len(uids))

        async with await factory.create_user_client(name="orchestrator") as client:
            handles = [Handle(AgentId(uid=uuid.UUID(u)), exchange=client, ignore_context=True) for u in uids]

            await probe.begin("idle")
            await asyncio.sleep(args.idle_s)
            await end_phase()

            for size_kb in args.sizes_kb if args.pattern == "broadcast" else []:
                payload = b"x" * (size_kb * 1024)
                await probe.begin(f"broadcast_{size_kb}kb")
                start = time.perf_counter()
                results = await asyncio.gather(*(asyncio.wait_for(h.broadcast(uids, payload), args.timeout)
                                                 for h in handles), return_exceptions=True)
                wall = time.perf_counter() - start
                ok = sum(r for r in results if isinstance(r, int))
                failed = [r for r in results if not isinstance(r, int)]
                # Peer echoes plus the N orchestrator -> agent broadcast requests.
                await end_phase(size_kb=size_kb, echoes=ok + len(results) - len(failed),
                                errors=len(uids) ** 2 - ok, broadcast_failures=len(failed),
                                error_types=sorted({type(e).__name__ for e in failed}),
                                throughput_per_s=ok / wall)

            for size_kb in args.sizes_kb if args.pattern == "echo" else []:
                payload = b"x" * (size_kb * 1024)
                await probe.begin(f"echo_{size_kb}kb")
                rounds = [await echo_round(handles, payload, args.inflight, args.timeout) for _ in range(args.rounds)]
                ok = sum(r["ok"] for r in rounds)
                await end_phase(size_kb=size_kb, echoes=ok, errors=sum(r["errors"] for r in rounds),
                                throughput_per_s=ok / sum(r["wall_s"] for r in rounds),
                                p50_s=statistics.median(r["p50_s"] for r in rounds if r["p50_s"] is not None)
                                if ok else None,
                                p99_s=max((r["p99_s"] for r in rounds if r["p99_s"] is not None), default=None),
                                rounds=rounds)

        await probe.begin("teardown")
        (run / "stop").touch()
        # terminate() SCANs the whole keyspace per agent (O(N^2)), so cap how long we wait.
        _, pending = await asyncio.wait([asyncio.create_task(asyncio.to_thread(p.wait)) for p in procs],
                                        timeout=args.teardown_timeout)
        await end_phase(timed_out=bool(pending), exit_codes=[p.poll() for p in procs])
    finally:
        (run / "stop").touch()
        for p in procs:
            if p.poll() is None:
                p.terminate()
        stop_metrics.set()
        await metrics
        await probe.stop()
        summarize(run, config, phases)


def summarize(run: Path, config: dict, phases: list[dict]) -> None:
    n = config["agents"]
    for p in phases:
        p["client"] = client_metrics(run, p["t_start"], p["t_end"])
        p["connected_clients_peak"] = max(
            [r["connected_clients"] for line in (run / "redis_info.jsonl").read_text().splitlines()
             if p["t_start"] <= (r := json.loads(line))["t"] <= p["t_end"]] + [p["connected_clients"] or 0])
        derived = {"redis_clients_per_agent": (p["connected_clients"] or 0) / n,
                   "redis_clients_peak_per_agent": p["connected_clients_peak"] / n,
                   "redis_client_mem_per_agent_bytes": (p["mem_clients_normal"] or 0) / n,
                   "redis_cmds_per_s": p["commands_total"] / p["wall_s"],
                   "redis_cpu_util": p["redis_cpu_s"] / p["wall_s"]}
        if p["phase"] == "idle":
            derived["redis_cmds_per_s_per_agent"] = derived["redis_cmds_per_s"] / n
        if p.get("echoes"):
            derived["redis_cmds_per_echo"] = p["commands_total"] / p["echoes"]
            derived["redis_cpu_ms_per_1k_echoes"] = 1e6 * p["redis_cpu_s"] / p["echoes"]
        p["derived"] = derived
    (run / "summary.json").write_text(json.dumps({"config": config, "phases": phases}, indent=2, default=str))
    print(f"wrote {run / 'summary.json'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--agents", type=int, default=100)
    parser.add_argument("--procs", type=int, default=2)
    parser.add_argument("--transport", choices=["tcp", "unix"], default="unix")
    parser.add_argument("--hosts", default="127.0.0.1", help="comma list; launcher i connects to hosts[i %% len]")
    parser.add_argument("--port", type=int, default=6391)
    parser.add_argument("--idle-s", type=float, default=60)
    parser.add_argument("--pattern", choices=["echo", "broadcast"], default="echo")
    parser.add_argument("--sizes-kb", type=lambda s: [int(x) for x in s.split(",")], default=[0, 1, 10])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--inflight", type=int, default=2000)
    parser.add_argument("--timeout", type=float, default=300, help="per-request timeout (s)")
    parser.add_argument("--socket-timeout", default="default",
                        help="redis-py socket timeout for exchange clients: default (5 s) | none | seconds")
    parser.add_argument("--launch-batch", type=int, default=500)
    parser.add_argument("--teardown-timeout", type=float, default=120)
    parser.add_argument("--launch-timeout", type=float, default=3600)
    asyncio.run(main(parser.parse_args()))
