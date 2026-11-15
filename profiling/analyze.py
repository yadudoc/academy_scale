"""Print one row per (run, phase) from runs/*/summary.json.  Usage: analyze.py [run_dir ...] (default: runs/*)"""

import json
import sys
from pathlib import Path

COLS = ["N", "P", "xport", "sock_to", "phase", "wall_s", "redis_cpu%", "cmds/s", "cmds/echo", "cpu_ms/1k",
        "clients/ag", "peak/ag", "rmem_KB/ag", "echo/s", "p50_ms", "p99_ms", "err", "lag_max_s", "crss_KB/ag", "top_cmds"]


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3g}"
    return str(v)


def rows(run: Path):
    s = json.loads((run / "summary.json").read_text())
    c, n = s["config"], s["config"]["agents"]
    for p in s["phases"]:
        d, cl = p["derived"], p["client"]
        yield [n, c["procs"], c["transport"], c.get("socket_timeout", "default"), p["phase"], p["wall_s"],
               100 * d["redis_cpu_util"], d["redis_cmds_per_s"], d.get("redis_cmds_per_echo"),
               d.get("redis_cpu_ms_per_1k_echoes"), d["redis_clients_per_agent"], d.get("redis_clients_peak_per_agent"),
               d["redis_client_mem_per_agent_bytes"] / 1024, p.get("throughput_per_s"),
               p["p50_s"] * 1e3 if p.get("p50_s") else None, p["p99_s"] * 1e3 if p.get("p99_s") else None,
               p.get("errors"), cl["launchers_lag_max_s"],
               cl["launchers_rss_total"] / n / 1024 if cl["launchers_rss_total"] else None,
               ",".join(f"{k}:{v['calls']}" for k, v in list(p["commandstats"].items())[:4])]


def main():
    runs = [Path(a) for a in sys.argv[1:]] or sorted(Path("runs").glob("*"))
    table = [COLS] + [[fmt(v) for v in r] for run in runs if (run / "summary.json").exists() for r in rows(run)]
    widths = [max(len(r[i]) for r in table) for i in range(len(COLS))]
    for r in table:
        print("  ".join(v.rjust(w) for v, w in zip(r, widths)))


if __name__ == "__main__":
    main()
