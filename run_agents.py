"""Launch N agents, one per process, and have the client message each one over a Redis exchange."""

import argparse
import asyncio
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

from academy.agent import Agent, action
from academy.exchange import RedisExchangeFactory
from academy.manager import Manager


class Echo(Agent):
    @action
    async def echo(self, msg: str) -> str:
        return f"pid {os.getpid()}: {msg}"


async def main(agents: int, redis_host: str, redis_port: int) -> None:
    # Each agent holds a worker for its lifetime, so workers == agents.
    pool = ProcessPoolExecutor(max_workers=agents, mp_context=multiprocessing.get_context("spawn"))
    # redis-py's async pool raises past 100 connections by default; size it for N concurrent calls.
    factory = RedisExchangeFactory(redis_host, redis_port, max_connections=agents + 10)
    async with await Manager.from_exchange_factory(factory=factory, executors=pool) as manager:
        handles = await asyncio.gather(*(manager.launch(Echo) for _ in range(agents)))
        replies = await asyncio.gather(*(h.echo(f"hello {i}") for i, h in enumerate(handles)))
    for reply in replies:
        print(reply)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agents", type=int, required=True, help="number of agents (= worker processes)")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()
    asyncio.run(main(args.agents, args.redis_host, args.redis_port))
