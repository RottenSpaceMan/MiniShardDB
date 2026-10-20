"""Compare indexed point lookups and SQLite page-cache work on existing data.

Never flushes the OS cache or modifies databases/filters. Timing and page probes
are separate so ctypes instrumentation does not distort application latency.
"""

import argparse
import csv
import json
import math
import platform
import random
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from collections.abc import Sequence
from contextlib import ExitStack, closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config
import executor
from coordinator import Coordinator
from database import connect_read_only
from experiments.sqlite_page_probe import PageProbe
from models import MergedResults, SqlRows
from shard import Shard

SQL = "SELECT * FROM orders WHERE order_id = ?"
WARM_SYSTEMS = (
    "central_reused",
    "central_fresh",
    "bloom_direct",
    "bloom_warm_pool",
    "broadcast_warm_pool",
)


@dataclass
class PointQueryRunner:
    """Execute benchmark paths without changing the coordinator's production policy."""

    central: sqlite3.Connection
    coordinator: Coordinator
    pool: executor.PersistentPool
    workers: int

    def execute(self, system: str, key: int) -> MergedResults:
        if system == "central_reused":
            return self.central.execute(SQL, (key,)).fetchall()
        if system == "central_fresh":
            with closing(connect_read_only(config.BASE_DB)) as connection:
                return connection.execute(SQL, (key,)).fetchall()
        if system == "bloom_direct":
            return self.coordinator.run(
                SQL, (key,), ("order_id", key), workers=self.workers
            )
        if system == "bloom_cold_pool":
            targets = self.coordinator.route(("order_id", key))
            results = executor.parallel(targets, SQL, (key,), workers=self.workers)
        elif system == "bloom_warm_pool":
            targets = self.coordinator.route(("order_id", key))
            results = self.pool.parallel(targets, SQL, (key,))
        elif system == "broadcast_warm_pool":
            results = self.pool.parallel(self.coordinator.shards, SQL, (key,))
        else:
            raise ValueError(f"unknown benchmark system: {system}")
        return self.coordinator.merge(results)


def sample_queries(
    connection: sqlite3.Connection, count: int, seed: int
) -> list[tuple[str, int]]:
    """Sample real IDs without assuming contiguous IDs; misses exceed MAX(id)."""
    ids = [
        row[0]
        for row in connection.execute("SELECT order_id FROM orders ORDER BY order_id")
    ]
    if not 1 <= count <= len(ids):
        raise ValueError("queries must be positive and no greater than the order count")
    rng = random.Random(seed)
    hits = rng.sample(ids, count)
    maximum = max(ids)
    return [("hit", key) for key in hits] + [
        ("miss", maximum + i) for i in range(1, count + 1)
    ]



def measure_pages(
    paths: Sequence[Path], key: int, warm: bool
) -> tuple[SqlRows, int, int]:
    """Sum statement-step cache counters across selected database connections."""
    rows, hits, misses = [], 0, 0
    for path in paths:
        with PageProbe(path) as probe:
            if warm:
                probe.execute(SQL, (key,))
            result, current_hits, current_misses = probe.execute(SQL, (key,))
            rows.extend(result)
            hits += current_hits
            misses += current_misses
    return rows, hits, misses


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(timings: list[dict], pages: list[dict]) -> dict:
    groups = defaultdict(list)
    for row in timings:
        groups[(row["workload"], row["system"])].append(row["seconds"])
    summary = {"latency": [], "pages": []}
    for (workload, system), seconds in sorted(groups.items()):
        ordered = sorted(seconds)
        summary["latency"].append({
            "workload": workload, "system": system, "samples": len(seconds),
            "median_ms": statistics.median(seconds) * 1000,
            "p95_ms": ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)] * 1000,
        })
    page_groups = defaultdict(list)
    for row in pages:
        page_groups[(row["workload"], row["system"], row["cache"])].append(row)
    for (workload, system, cache), rows in sorted(page_groups.items()):
        summary["pages"].append({
            "workload": workload, "system": system, "cache": cache,
            "queries": len(rows),
            "selected_databases": sum(row["selected_databases"] for row in rows),
            "cache_hits": sum(row["cache_hits"] for row in rows),
            "cache_misses": sum(row["cache_misses"] for row in rows),
        })
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", type=int, default=100, help="IDs per hit/miss workload")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument(
        "--cold-queries", type=int, default=5,
        help="IDs per workload timed with a new pool",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-prefix", default="point-query",
        help="Output filename prefix; use a new name to preserve earlier evidence",
    )
    args = parser.parse_args(argv)
    if not args.output_prefix or Path(args.output_prefix).name != args.output_prefix:
        parser.error("--output-prefix must be a filename prefix, not a path")
    if args.queries < 1 or args.repetitions < 1 or args.workers < 1 or args.cold_queries < 0:
        parser.error("counts must be positive; --cold-queries may be zero")
    paths = [config.shard_path(i) for i in range(config.NUM_SHARDS)]
    for path in [config.BASE_DB, *paths]:
        if not path.is_file():
            parser.error(f"missing database: {path}; run gen_data.py first")

    return args


def query_plans(paths: Sequence[Path], key: int) -> dict[str, SqlRows]:
    """Collect plans without retaining extra connections until the run ends."""
    plans = {}
    for path in paths:
        with closing(connect_read_only(path)) as connection:
            plans[str(path.relative_to(PROJECT_ROOT))] = connection.execute(
                "EXPLAIN QUERY PLAN " + SQL, (key,)
            ).fetchall()
    return plans


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    paths = [config.shard_path(i) for i in range(config.NUM_SHARDS)]
    timings, pages = [], []
    rng = random.Random(args.seed)
    with ExitStack() as resources:
        central = resources.enter_context(closing(connect_read_only(config.BASE_DB)))
        shards = [resources.enter_context(Shard(str(path))) for path in paths]
        if any(shard.bloom is None for shard in shards):
            argparse.ArgumentParser(description=__doc__).error(
                "all shards need saved Bloom filters for this comparison"
            )
        coordinator = Coordinator(shards)
        queries = sample_queries(central, args.queries, args.seed)
        expected = {key: central.execute(SQL, (key,)).fetchall() for _, key in queries}
        pool = resources.enter_context(executor.PersistentPool(args.workers))

        runner = PointQueryRunner(central, coordinator, pool, args.workers)
        # An unmeasured broadcast starts the workers; exercise each measured path.
        pool.parallel(shards, SQL, (queries[0][1],))
        for _, key in queries:
            for system in WARM_SYSTEMS:
                if runner.execute(system, key) != expected[key]:
                    raise RuntimeError(f"warmup result mismatch: {system}, {key}")
        for repetition in range(1, args.repetitions + 1):
            jobs = [
                (workload, key, system)
                for workload, key in queries
                for system in WARM_SYSTEMS
            ]
            rng.shuffle(jobs)
            for workload, key, system in jobs:
                start = time.perf_counter()
                result = runner.execute(system, key)
                seconds = time.perf_counter() - start
                if result != expected[key]:
                    raise RuntimeError(f"result mismatch: {system}, {key}")
                timings.append({"workload": workload, "order_id": key, "system": system,
                                "repetition": repetition, "seconds": seconds})
            print(f"Completed warm repetition {repetition}/{args.repetitions}", flush=True)
        for workload in ("hit", "miss"):
            subset = [query for query in queries if query[0] == workload][:args.cold_queries]
            for _, key in subset:
                start = time.perf_counter()
                result = runner.execute("bloom_cold_pool", key)
                seconds = time.perf_counter() - start
                if result != expected[key]:
                    raise RuntimeError(f"cold result mismatch: {key}")
                timings.append({"workload": workload, "order_id": key, "system": "bloom_cold_pool",
                                "repetition": 1, "seconds": seconds})
        print("Measuring page-cache counters separately...", flush=True)
        for workload, key in queries:
            targets = coordinator.route(("order_id", key))
            for system, selected in (
                ("central", [config.BASE_DB]),
                ("bloom", [Path(target.path) for target in targets]),
                ("broadcast", paths),
            ):
                for warm in (False, True):
                    result, hits, misses = measure_pages(selected, key, warm)
                    if result != expected[key]:
                        raise RuntimeError(f"page probe result mismatch: {system}, {key}")
                    pages.append({"workload": workload, "order_id": key, "system": system,
                                  "cache": "primed_same_query" if warm else "fresh_connection",
                                  "selected_databases": len(selected), "cache_hits": hits, "cache_misses": misses})
        manifest = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "arguments": vars(args), "python": sys.version, "platform": platform.platform(),
            "sqlite_python": sqlite3.sqlite_version, "sqlite_probe": PageProbe.sqlite_version,
            "orders": central.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "shards": len(shards), "sql": SQL,
            "plans": query_plans([config.BASE_DB, *paths], queries[0][1]),
            "filter_bytes": sum(len(shard.bloom.bits) for shard in shards if shard.bloom is not None),
            "execution_paths": {
                "bloom_direct": "Coordinator.run: sequential selected shards using existing connections",
                "bloom_warm_pool": "Legacy routed execution: persistent processes, fresh shard connections",
                "bloom_cold_pool": "Legacy routed execution: new process pool, fresh shard connections",
                "broadcast_warm_pool": "All shards: persistent processes, fresh shard connections",
            },
            "notes": ["No OS cache flush; not physical disk I/O",
                      "Page counters exclude prepare-time activity; primed means repeat same query",
                      "Warm pool still opens/closes SQLite connections for each shard job; direct path reuses Shard connections",
                      "Timings include routing, dispatch, fetch, merge; exclude initial filter load, auth and CLI",
                      "Warm timing jobs shuffled; cold subset measured once after warm timings",
                      "No outlier rejection; p95 uses nearest rank"],
        }
    output = PROJECT_ROOT / "report" / "evidence"
    output.mkdir(parents=True, exist_ok=True)
    prefix = args.output_prefix
    write_csv(output / f"{prefix}-timings.csv", timings)
    write_csv(output / f"{prefix}-pages.csv", pages)
    summary = summarize(timings, pages)
    for suffix, content in (("manifest", manifest), ("summary", summary)):
        (output / f"{prefix}-{suffix}.json").write_text(json.dumps(content, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
