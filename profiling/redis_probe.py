"""Redis server-side instrumentation: 1 Hz INFO samples plus per-phase command/CPU/latency snapshots.

Avoids CLIENT LIST and other O(clients) commands, which would themselves stall Redis at 100K clients.
"""

import asyncio
import json
import time
from pathlib import Path

import redis.asyncio as redis

SAMPLE_SECTIONS = ("clients", "memory", "stats", "cpu")


class RedisProbe:
    def __init__(self, client: redis.Redis, run_dir: Path):
        self.r = client
        self.samples = open(run_dir / "redis_info.jsonl", "a")
        self._task: asyncio.Task | None = None
        self._phase = "init"
        self._begin: dict = {}

    async def _info(self) -> dict:
        return await self.r.execute_command("INFO", *SAMPLE_SECTIONS)

    async def _sample_loop(self) -> None:
        while True:
            info = await self._info()
            self.samples.write(json.dumps({"t": time.time(), "phase": self._phase, **info}) + "\n")
            self.samples.flush()
            await asyncio.sleep(1)

    def start(self) -> None:
        self._task = asyncio.create_task(self._sample_loop())

    async def stop(self) -> None:
        self._task.cancel()
        self.samples.close()

    async def begin(self, phase: str) -> None:
        """Zero Redis' counters so the phase's commandstats/slowlog/latency are its own."""
        self._phase = phase
        await self.r.config_resetstat()
        await self.r.slowlog_reset()
        await self.r.execute_command("LATENCY", "RESET")
        info = await self._info()
        self._begin = {"t": time.time(), "cpu_s": _cpu(info), "cpu_main_s": _cpu(info, "_main_thread")}

    async def end(self) -> dict:
        """Snapshot Redis state for the phase that began with begin()."""
        t = time.time()
        info = await self._info()
        cmdstats = await self.r.info("commandstats")
        latency = await self.r.execute_command("LATENCY", "LATEST")
        slowlog = await self.r.slowlog_get(1024)
        return {
            "phase": self._phase,
            "t_start": self._begin["t"],
            "t_end": t,
            "wall_s": t - self._begin["t"],
            "redis_cpu_s": _cpu(info) - self._begin["cpu_s"],
            "redis_cpu_main_thread_s": _cpu(info, "_main_thread") - self._begin["cpu_main_s"],
            "commands_total": sum(v["calls"] for v in cmdstats.values()),
            "commandstats": {k.removeprefix("cmdstat_"): {"calls": v["calls"], "usec": v["usec"]}
                             for k, v in sorted(cmdstats.items(), key=lambda kv: -kv[1]["calls"])},
            **{k: info.get(k) for k in ("connected_clients", "blocked_clients", "used_memory",
                                         "mem_clients_normal", "rejected_connections", "total_commands_processed",
                                         "total_connections_received")},
            "slowlog_count": len(slowlog),
            "slowlog_top": [{"cmd": str(e["command"])[:80], "us": e["duration"]} for e in slowlog[:5]],
            "latency_latest": [list(map(str, row)) for row in latency],
        }


def _cpu(info: dict, suffix: str = "") -> float:
    return float(info.get(f"used_cpu_user{suffix}", 0)) + float(info.get(f"used_cpu_sys{suffix}", 0))
