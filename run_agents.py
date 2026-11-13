"""Sweep agent counts (even steps, one process per agent) and payload sizes over a Redis exchange.

For each agent count N: launch N agents, then for each payload size echo it client -> agent -> client
on every agent at once, REPEATS times. Per repeat: dispatch_s = until all N requests are sent,
complete_s = until all N results are back (includes dispatch). Timings are written to a JSON file.
"""

import argparse
import asyncio
import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor

from academy.agent import Agent, action
from academy.exchange import RedisExchangeFactory
from academy.manager import Manager

REPEATS = 3


class Echo(Agent):
    @action
    async def echo(self, payload: bytes) -> bytes:
        return payload


async def run_step(agents: int, sizes_kb: list[int], host: str, port: int) -> list[dict]:
    # Each agent holds a worker for its lifetime, so workers == agents.
    pool = ProcessPoolExecutor(max_workers=agents, mp_context=multiprocessing.get_context("spawn"))
    # redis-py's async pool raises past 100 connections by default; size it for N concurrent calls.
    factory = RedisExchangeFactory(host, port, max_connections=agents + 10)
    records = []
    async with await Manager.from_exchange_factory(factory=factory, executors=pool) as manager:
        start = time.perf_counter()
        handles = await asyncio.gather(*(manager.launch(Echo) for _ in range(agents)))
        launch_s = time.perf_counter() - start

        # Handles send requests through the manager's client; count sends to time dispatch separately.
        client, sent, all_sent = manager.exchange_client, 0, asyncio.Event()
        send = client.send

        async def counting_send(message):
            nonlocal sent
            await send(message)
            sent += 1
            if sent == agents:
                all_sent.set()

        client.send = counting_send
        for size_kb in sizes_kb:
            payload = b"x" * (size_kb * 1024)
            dispatch, complete = [], []
            for _ in range(REPEATS):
                sent = 0
                all_sent.clear()
                start = time.perf_counter()
                actions = asyncio.gather(*(h.echo(payload) for h in handles))
                await all_sent.wait()
                dispatch.append(time.perf_counter() - start)
                await actions
                complete.append(time.perf_counter() - start)
            records.append({"agents": agents, "size_kb": size_kb, "launch_s": launch_s,
                            "dispatch_s": dispatch, "complete_s": complete})
            print(records[-1], flush=True)
    return records


async def main(args: argparse.Namespace) -> None:
    # Every even count in range, plus the start itself (so a start of 1 is kept).
    counts = [n for n in range(args.min_agents, args.max_agents + 1) if n == args.min_agents or n % 2 == 0]
    results = []
    for agents in counts:
        results += await run_step(agents, args.sizes_kb, args.redis_host, args.redis_port)
        with open(args.out, "w") as f:  # rewrite after each count so a crash keeps finished steps
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-agents", type=int, default=2)
    parser.add_argument("--max-agents", type=int, default=128)
    parser.add_argument("--sizes-kb", type=lambda s: [int(x) for x in s.split(",")], default=[0, 1, 10, 100])
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--out", default="results.json")
    asyncio.run(main(parser.parse_args()))
