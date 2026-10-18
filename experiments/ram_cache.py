"""Compare file-backed and in-memory SQLite query execution."""

import argparse
import csv
import sqlite3
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config

BASE_DB: Path = PROJECT_ROOT / config.BASE_DB
SHARD_DIR: Path = PROJECT_ROOT / config.SHARD_DIR
DEFAULT_QUERY: str = "SELECT status, SUM(amount) FROM orders GROUP BY status"


def measure_file_backed(
    query: str,
    repetitions: int,
) -> tuple[list[float], list[tuple[object, ...]]]:
    """Measure repeated aggregate queries against the centralized database file."""
    timings = []
    result: list[tuple[object, ...]] = []
    for _ in range(repetitions):
        start = time.perf_counter()
        with sqlite3.connect(BASE_DB) as connection:
            result = connection.execute(query).fetchall()
        timings.append(time.perf_counter() - start)
    return timings, result


def measure_memory_backed(
    query: str,
    repetitions: int,
) -> tuple[float, list[float], list[tuple[object, ...]]]:
    """Copy the centralized database to RAM and measure repeated queries."""
    start = time.perf_counter()
    with sqlite3.connect(BASE_DB) as disk_connection:
        memory_connection = sqlite3.connect(
            ":memory:", check_same_thread=False
        )
        disk_connection.backup(memory_connection)
    load_seconds = time.perf_counter() - start

    timings = []
    result: list[tuple[object, ...]] = []
    try:
        for _ in range(repetitions):
            start = time.perf_counter()
            result = memory_connection.execute(query).fetchall()
            timings.append(time.perf_counter() - start)
    finally:
        memory_connection.close()
    return load_seconds, timings, result


def shard_paths() -> list[Path]:
    """Return the expected paths for all shard database files."""
    return [PROJECT_ROOT / config.shard_path(index) for index in range(config.NUM_SHARDS)]


def merge_grouped(
    shard_results: list[list[tuple[object, ...]]],
) -> list[tuple[object, ...]]:
    """Merge grouped numeric results from all shards deterministically."""
    totals: dict[object, float] = {}
    for rows in shard_results:
        for key, value in rows:
            if not isinstance(value, (int, float)):
                raise TypeError("grouped shard values must be numeric")
            totals[key] = totals.get(key, 0.0) + value
    return sorted(totals.items(), key=lambda item: str(item[0]))


def measure_sharded_file_backed(
    query: str,
    repetitions: int,
) -> tuple[list[float], list[tuple[object, ...]]]:
    """Measure repeated queries against file-backed shards."""
    timings = []
    result: list[tuple[object, ...]] = []
    paths = shard_paths()
    for _ in range(repetitions):
        start = time.perf_counter()
        connections = [sqlite3.connect(path) for path in paths]
        try:
            result = merge_grouped(
                [connection.execute(query).fetchall() for connection in connections]
            )
        finally:
            for connection in connections:
                connection.close()
        timings.append(time.perf_counter() - start)
    return timings, result


def measure_sharded_memory_backed(
    query: str,
    repetitions: int,
) -> tuple[float, list[float], list[tuple[object, ...]]]:
    """Copy all shards to RAM and measure concurrent repeated queries."""
    start = time.perf_counter()
    connections: list[sqlite3.Connection] = []
    try:
        for path in shard_paths():
            with sqlite3.connect(path) as disk_connection:
                memory_connection = sqlite3.connect(
                    ":memory:", check_same_thread=False
                )
                disk_connection.backup(memory_connection)
                connections.append(memory_connection)
    except Exception:
        for connection in connections:
            connection.close()
        raise
    load_seconds = time.perf_counter() - start

    timings = []
    result: list[tuple[object, ...]] = []
    try:
        for _ in range(repetitions):
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=config.NUM_SHARDS) as pool:
                futures = [
                    pool.submit(connection.execute, query)
                    for connection in connections
                ]
                result = merge_grouped([future.result().fetchall() for future in futures])
            timings.append(time.perf_counter() - start)
    finally:
        for connection in connections:
            connection.close()
    return load_seconds, timings, result


def write_results(rows: list[dict[str, object]]) -> Path:
    """Write raw RAM-cache measurements to the evidence CSV."""
    output_path = PROJECT_ROOT / "report" / "evidence" / "ram-cache.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=["system", "measurement", "repetition", "seconds"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> None:
    """Run centralized and sharded file/RAM comparisons."""
    parser = argparse.ArgumentParser(
        description="Compare file-backed SQLite with an in-memory SQLite copy."
    )
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--query", default=DEFAULT_QUERY)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    databases = [BASE_DB, *shard_paths()]
    missing = [str(path) for path in databases if not path.exists()]
    if missing:
        parser.error("missing databases; run gen_data.py first: " + ", ".join(missing))

    file_timings, file_result = measure_file_backed(
        args.query, args.repetitions
    )
    load_seconds, memory_timings, memory_result = measure_memory_backed(
        args.query, args.repetitions
    )
    if file_result != memory_result:
        raise RuntimeError("centralized file-backed and in-memory results differ")
    sharded_file_timings, sharded_file_result = measure_sharded_file_backed(
        args.query, args.repetitions
    )
    sharded_load_seconds, sharded_memory_timings, sharded_memory_result = (
        measure_sharded_memory_backed(args.query, args.repetitions)
    )
    if sharded_file_result != sharded_memory_result:
        raise RuntimeError("sharded file-backed and in-memory results differ")

    rows: list[dict[str, object]] = [
        {
            "system": "file-backed SQLite",
            "measurement": "query",
            "repetition": index,
            "seconds": f"{seconds:.6f}",
        }
        for index, seconds in enumerate(file_timings, start=1)
    ]
    rows.extend(
        {
            "system": "in-memory SQLite",
            "measurement": "query",
            "repetition": index,
            "seconds": f"{seconds:.6f}",
        }
        for index, seconds in enumerate(memory_timings, start=1)
    )
    rows.extend(
        {
            "system": "sharded file-backed SQLite",
            "measurement": "query",
            "repetition": index,
            "seconds": f"{seconds:.6f}",
        }
        for index, seconds in enumerate(sharded_file_timings, start=1)
    )
    rows.extend(
        {
            "system": "sharded in-memory SQLite",
            "measurement": "query",
            "repetition": index,
            "seconds": f"{seconds:.6f}",
        }
        for index, seconds in enumerate(sharded_memory_timings, start=1)
    )
    rows.append(
        {
            "system": "in-memory SQLite",
            "measurement": "load",
            "repetition": 1,
            "seconds": f"{load_seconds:.6f}",
        }
    )
    rows.append(
        {
            "system": "sharded in-memory SQLite",
            "measurement": "load",
            "repetition": 1,
            "seconds": f"{sharded_load_seconds:.6f}",
        }
    )
    output_path = write_results(rows)

    print(f"File-backed query median: {statistics.median(file_timings):.6f} seconds")
    print(f"In-memory load time:       {load_seconds:.6f} seconds")
    print(f"In-memory query median:    {statistics.median(memory_timings):.6f} seconds")
    print(f"In-memory total:           {load_seconds + sum(memory_timings):.6f} seconds")
    print(
        "Sharded file query median: "
        f"{statistics.median(sharded_file_timings):.6f} seconds"
    )
    print(f"Sharded RAM load time:      {sharded_load_seconds:.6f} seconds")
    print(
        "Sharded RAM query median:   "
        f"{statistics.median(sharded_memory_timings):.6f} seconds"
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
