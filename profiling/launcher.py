"""One launcher process: run K Echo agents on a single event loop (Manager, no executors).

Writes the agent UIDs to <run_dir>/agents/<index>.json once launched, samples client-side metrics
(event-loop lag, RSS, fds, CPU) to <run_dir>/client_launcher<index>.jsonl, and shuts the agents
down when <run_dir>/stop appears.
"""

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path

import psutil
from academy.agent import Agent, action
from academy.exchange import RedisExchangeFactory
from academy.handle import Handle
from academy.identifier import AgentId
from academy.manager import Manager


class Echo(Agent):
    @action
    async def echo(self, payload: bytes) -> bytes:
        return payload

    @action
    async def broadcast(self, peers: list[str], payload: bytes) -> int:
        """Echo payload off every peer concurrently (this agent's exchange client); return #successes."""
        handles = [Handle(AgentId(uid=uuid.UUID(p))) for p in peers]
        results = await asyncio.gather(*(h.echo(payload) for h in handles), return_exceptions=True)
        return sum(not isinstance(r, BaseException) for r in results)


def make_factory(transport: str, host: str, port: int, sock: str, **redis_kwargs) -> RedisExchangeFactory:
    # Every transport the factory creates (one per agent) opens its own connection(s) to this endpoint.
    if transport == "unix":
        redis_kwargs["unix_socket_path"] = sock
    return RedisExchangeFactory(host, port, **redis_kwargs)


def socket_timeout_kwargs(value: str) -> dict:
    """'default' keeps redis-py's 5 s socket timeout, which cuts every idle BLPOP (block forever) after 5 s
    and makes the client reconnect + re-issue it; 'none' disables it; a number sets it in seconds."""
    if value == "default":
        return {}
    return {"socket_timeout": None if value == "none" else float(value)}


async def sample_metrics(path: Path, stop: asyncio.Event) -> None:
    """Once a second, append max event-loop lag (sleep overshoot), RSS, open fds and CPU seconds."""
    proc = psutil.Process()
    with open(path, "a") as f:
        while not stop.is_set():
            lag = 0.0
            for _ in range(10):
                start = time.perf_counter()
                await asyncio.sleep(0.1)
                lag = max(lag, time.perf_counter() - start - 0.1)
            cpu = proc.cpu_times()
            f.write(json.dumps({"t": time.time(), "lag_max_s": lag, "rss": proc.memory_info().rss,
                                "fds": proc.num_fds(), "cpu_s": cpu.user + cpu.system}) + "\n")
            f.flush()


async def main(args: argparse.Namespace) -> None:
    run = Path(args.run_dir)
    stop = asyncio.Event()
    metrics = asyncio.create_task(sample_metrics(run / f"client_launcher{args.index}.jsonl", stop))
    # The Manager registers a whole batch concurrently through one client; redis-py's pool caps at 100 by default.
    factory = make_factory(args.transport, args.host, args.port, str(run / "redis.sock"), max_connections=args.batch + 64,
                           **socket_timeout_kwargs(args.socket_timeout))
    async with await Manager.from_exchange_factory(factory=factory) as manager:
        handles = []
        for i in range(0, args.agents, args.batch):  # batches avoid a connect storm on Redis' accept queue
            n = min(args.batch, args.agents - i)
            handles += await asyncio.gather(*(manager.launch(Echo) for _ in range(n)))
        tmp = run / "agents" / f"{args.index}.json.tmp"
        tmp.write_text(json.dumps([str(h.agent_id.uid) for h in handles]))
        tmp.rename(tmp.with_suffix(""))  # atomic: orchestrator only sees complete files
        while not (run / "stop").exists():
            await asyncio.sleep(0.5)
    stop.set()
    await metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--agents", type=int, required=True)
    parser.add_argument("--transport", choices=["tcp", "unix"], default="unix")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6391)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--socket-timeout", default="default", help="default | none | seconds")
    asyncio.run(main(parser.parse_args()))
