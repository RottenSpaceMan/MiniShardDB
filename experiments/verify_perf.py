"""Benchmark cold-start and warm-pool MiniShardDB execution."""

import argparse
import csv
import math
import sqlite3
import sys
import time
from pathlib import Path
from collections.abc import Callable

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config
import executor
from coordinator import Coordinator
from parser import has_group_by, parse_where
from shard import Shard


DEFAULT_WORKERS: list[int] = [1, 2, 4, 8]
DEFAULT_SQL: str = "SELECT status, SUM(amount) FROM orders GROUP BY status"


def parse_worker_series(value: str) -> list[int]:
    """Parse a comma-separated positive worker-count list."""
    try:
        workers = [int(item.strip()) for item in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "workers must be a comma-separated list of positive integers"
        ) from error
    if not workers or any(worker < 1 for worker in workers):
        raise argparse.ArgumentTypeError(
            "workers must be a comma-separated list of positive integers"
        )
    return workers


def timed(function: Callable[[], object]) -> tuple[float, object]:
    """Return a callable's result and elapsed wall-clock time."""
    start = time.perf_counter()
    result = function()
    return time.perf_counter() - start, result


def run_cold(
    coordinator: Coordinator,
    query: str,
    worker_count: int,
) -> tuple[float, object]:
    """Measure one cold-start coordinator execution."""
    return timed(
        lambda: coordinator.run(query, (), parse_where(query), workers=worker_count)
    )


def run_warm(
    coordinator: Coordinator,
    pool: executor.PersistentPool,
    query: str,
) -> tuple[float, object]:
    """Measure one query using an already-running worker pool."""
    targets = coordinator.route(parse_where(query))
    start = time.perf_counter()
    results = pool.parallel(targets, query, ())
    elapsed = time.perf_counter() - start
    return elapsed, coordinator.merge(results, grouped=has_group_by(query))


def run_centralized(
    connection: sqlite3.Connection,
    query: str,
) -> tuple[float, list[tuple[object, ...]]]:
    """Measure one query on a reused centralized SQLite connection."""
    start = time.perf_counter()
    result = connection.execute(query).fetchall()
    return time.perf_counter() - start, result


def run_centralized_cold(
    database_path: Path,
    query: str,
) -> tuple[float, list[tuple[object, ...]]]:
    """Measure one query with a fresh centralized SQLite connection."""
    def execute() -> list[tuple[object, ...]]:
        with sqlite3.connect(database_path) as connection:
            return connection.execute(query).fetchall()

    start = time.perf_counter()
    result = execute()
    return time.perf_counter() - start, result


def comparable_result(result: object, grouped: bool) -> object:
    """Normalize grouped coordinator results for comparison."""
    if not grouped:
        return result
    if isinstance(result, dict):
        return result
    if not isinstance(result, list):
        raise TypeError("grouped query returned an unsupported result type")
    return {row[0]: row[1] for row in result}


def results_match(left: object, right: object, grouped: bool) -> bool:
    """Compare query results while tolerating grouped float round-off."""
    left = comparable_result(left, grouped)
    right = comparable_result(right, grouped)
    if grouped and isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(
            math.isclose(float(left[key]), float(right[key]), rel_tol=1e-12)
            for key in left
        )
    return left == right


def append_rows(
    rows: list[dict[str, object]],
    system: str,
    mode: str,
    workers: int,
    repetition: int,
    seconds: float,
) -> None:
    """Append one normalized benchmark measurement to the output rows."""
    rows.append(
        {
            "system": system,
            "mode": mode,
            "workers": workers,
            "repetition": repetition,
            "seconds": f"{seconds:.6f}",
        }
    )


def main() -> None:
    """Run the selected cold/warm MiniShardDB and centralized benchmarks."""
    parser = argparse.ArgumentParser(
        description="Compare cold-start and warm-pool SQLite query performance."
    )
    parser.add_argument(
        "--worker-series",
        type=parse_worker_series,
        default=DEFAULT_WORKERS,
        help="Comma-separated worker counts (default: 1,2,4,8)",
    )
    parser.add_argument("--override-sql", default=DEFAULT_SQL)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument(
        "--mode",
        choices=("cold", "warm", "both"),
        default="both",
        help="Benchmark cold-start, warm-pool, or both modes",
    )
    args = parser.parse_args()
    if args.repetitions < 1 or args.warmups < 0:
        parser.error("--repetitions must be positive and --warmups cannot be negative")

    base_path = PROJECT_ROOT / config.BASE_DB
    shard_paths = [
        PROJECT_ROOT / config.SHARD_DIR / f"shard{i}.db"
        for i in range(config.NUM_SHARDS)
    ]
    missing = [str(path) for path in [base_path, *shard_paths] if not path.exists()]
    if missing:
        parser.error("missing databases; run gen_data.py first: " + ", ".join(missing))

    shards = [Shard(str(path)) for path in shard_paths]
    coordinator = Coordinator(shards)
    rows: list[dict[str, object]] = []
    expected_result: object | None = None
    grouped = has_group_by(args.override_sql)

    try:
        with sqlite3.connect(base_path) as central_connection:
            for worker_count in args.worker_series:
                if args.mode in ("cold", "both"):
                    print(f"Cold-start MiniShardDB with {worker_count} workers...")
                    for _ in range(args.warmups):
                        _, expected_result = run_cold(
                            coordinator, args.override_sql, worker_count
                        )
                    for repetition in range(1, args.repetitions + 1):
                        seconds, result = run_cold(
                            coordinator, args.override_sql, worker_count
                        )
                        comparable = comparable_result(result, grouped)
                        if expected_result is None:
                            expected_result = comparable
                        if not results_match(comparable, expected_result, grouped):
                            raise RuntimeError("inconsistent MiniShardDB result")
                        append_rows(
                            rows, "MiniShardDB", "cold", worker_count,
                            repetition, seconds,
                        )
                        print(f"  run {repetition}: {seconds:.6f} seconds")

                if args.mode in ("warm", "both"):
                    print(f"Warm-pool MiniShardDB with {worker_count} workers...")
                    with executor.PersistentPool(worker_count) as pool:
                        for _ in range(args.warmups):
                            _, expected_result = run_warm(
                                coordinator, pool, args.override_sql
                            )
                        for repetition in range(1, args.repetitions + 1):
                            seconds, result = run_warm(
                                coordinator, pool, args.override_sql
                            )
                            comparable = comparable_result(result, grouped)
                            if expected_result is None:
                                expected_result = comparable
                            if not results_match(comparable, expected_result, grouped):
                                raise RuntimeError("inconsistent MiniShardDB result")
                            append_rows(
                                rows, "MiniShardDB", "warm", worker_count,
                                repetition, seconds,
                            )
                            print(f"  run {repetition}: {seconds:.6f} seconds")

            if args.mode in ("cold", "both"):
                print("Cold-start centralized SQLite...")
                for repetition in range(1, args.repetitions + 1):
                    seconds, result = run_centralized_cold(
                        base_path, args.override_sql
                    )
                    if not results_match(result, expected_result, grouped):
                        raise RuntimeError("centralized result differs from MiniShardDB")
                    append_rows(rows, "centralized SQLite", "cold", 1, repetition, seconds)
                    print(f"  run {repetition}: {seconds:.6f} seconds")

            if args.mode in ("warm", "both"):
                print("Warm-connection centralized SQLite...")
                for _ in range(args.warmups):
                    run_centralized(central_connection, args.override_sql)
                for repetition in range(1, args.repetitions + 1):
                    seconds, result = run_centralized(
                        central_connection, args.override_sql
                    )
                    if not results_match(result, expected_result, grouped):
                        raise RuntimeError("centralized result differs from MiniShardDB")
                    append_rows(rows, "centralized SQLite", "warm", 1, repetition, seconds)
                    print(f"  run {repetition}: {seconds:.6f} seconds")
    finally:
        for shard in shards:
            shard.close()

    output_path = PROJECT_ROOT / "report" / "evidence" / "e2-e3-performance.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=["system", "mode", "workers", "repetition", "seconds"],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
