"""Benchmark shard-count and dataset-size scaling and generate two chart matrices."""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import executor
import config
from coordinator import Coordinator
from shard import Shard


DEFAULT_ROWS = (100_000, 1_000_000, 5_000_000, 10_000_000)
DEFAULT_SHARDS = (2, 4, 8, 16)
DEFAULT_WORKERS = (1, 2, 4, 8)
STATUSES = config.ORDER_STATUSES
QUERY = "SELECT status, SUM(amount) FROM orders GROUP BY status"
DATASET_ROOT = PROJECT_ROOT / "data" / "benchmark_matrix"
EVIDENCE_ROOT = PROJECT_ROOT / "report" / "evidence"
FIGURES_ROOT = PROJECT_ROOT / "report" / "figures"


def log(message: str) -> None:
    """Print a timestamped progress message immediately."""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def parse_series(value: str, label: str) -> tuple[int, ...]:
    """Parse a comma-separated series of positive integers."""
    try:
        values = tuple(int(item.strip()) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"{label} must be a comma-separated list of positive integers"
        ) from error
    if not values or any(value < 1 for value in values):
        raise argparse.ArgumentTypeError(
            f"{label} must be a comma-separated list of positive integers"
        )
    return values


def parse_size_series(value: str) -> tuple[int, ...]:
    """Parse dataset sizes, accepting k/m suffixes."""
    sizes = []
    for item in value.split(","):
        text = item.strip().lower()
        multiplier = 1
        if text.endswith("m"):
            multiplier = 1_000_000
            text = text[:-1]
        elif text.endswith("k"):
            multiplier = 1_000
            text = text[:-1]
        try:
            size = int(text) * multiplier
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                "row sizes must be integers with optional k or m suffixes"
            ) from error
        if size < 1:
            raise argparse.ArgumentTypeError("row sizes must be positive")
        sizes.append(size)
    return tuple(sizes)


def dataset_path(rows: int, shard_count: int) -> Path:
    return DATASET_ROOT / f"rows-{rows}" / f"shards-{shard_count}"


def order_batch(
    bounds: tuple[int, int, int],
) -> list[tuple[int, int, float, str, str]]:
    """Generate one deterministic order batch in a worker process."""
    start, end, shard_count = bounds
    orders = []
    for order_id in range(start, end):
        customer_id = ((order_id - 1) % (shard_count * 10_000)) + 1
        amount = float((order_id * 17) % 49_001 + 100) / 100
        status = STATUSES[order_id % len(STATUSES)]
        orders.append((order_id, customer_id, amount, "2026-01-01", status))
    return orders


def batch_bounds(rows: int, shard_count: int, batch_size: int) -> list[tuple[int, int, int]]:
    """Return inclusive-exclusive order ranges for worker batches."""
    return [
        (start, min(start + batch_size, rows + 1), shard_count)
        for start in range(1, rows + 1, batch_size)
    ]


def build_dataset(
    rows: int,
    shard_count: int,
    batch_size: int,
    generation_workers: int,
) -> Path:
    """Create one isolated shard set if it does not already exist."""
    root = dataset_path(rows, shard_count)
    marker = root / ".complete"
    if marker.exists():
        log(f"DATASET reuse: {rows:,} rows / {shard_count} shards")
        return root

    for partial_path in root.glob("shard*.db"):
        partial_path.unlink()
    started = time.perf_counter()
    log(
        f"DATASET start: {rows:,} rows / {shard_count} shards "
        f"(batch={batch_size:,}, pool={generation_workers})"
    )
    root.mkdir(parents=True, exist_ok=True)
    connections = []
    try:
        for shard_index in range(shard_count):
            path = root / f"shard{shard_index}.db"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE orders (
                    order_id INTEGER PRIMARY KEY,
                    cust_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                )
                """
            )
            connections.append(connection)

        bounds = batch_bounds(rows, shard_count, batch_size)
        with ProcessPoolExecutor(max_workers=generation_workers) as pool:
            for batch_number, batch in enumerate(
                pool.map(order_batch, bounds), start=1
            ):
                grouped: dict[int, list[tuple[int, int, float, str, str]]] = {}
                for order in batch:
                    grouped.setdefault((order[1] - 1) % shard_count, []).append(order)
                for shard_index, shard_rows in grouped.items():
                    connections[shard_index].executemany(
                        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)", shard_rows
                    )
                for connection in connections:
                    connection.commit()
                if batch_number == 1 or batch_number == len(bounds) or batch_number % 10 == 0:
                    log(f"DATASET progress: batch {batch_number}/{len(bounds)}")

    finally:
        for connection in connections:
            connection.close()

    marker.write_text("complete\n", encoding="ascii")
    log(f"DATASET complete: {rows:,} rows / {shard_count} shards in "
        f"{time.perf_counter() - started:.1f}s")
    return root


def run_query(root: Path, worker_count: int) -> float:
    """Measure one grouped aggregation over all shards."""
    paths = sorted(root.glob("shard*.db"), key=lambda path: int(path.stem[5:]))
    shards = [Shard(str(path)) for path in paths]
    coordinator = Coordinator(shards)
    try:
        start = time.perf_counter()
        result = coordinator.run(QUERY, (), None, workers=worker_count)
        elapsed = time.perf_counter() - start
        if not result:
            raise RuntimeError("benchmark query returned no result")
        return elapsed
    finally:
        for shard in shards:
            shard.close()


def run_warm_repetitions(root: Path, worker_count: int, repetitions: int) -> list[float]:
    paths = sorted(root.glob("shard*.db"), key=lambda path: int(path.stem[5:]))
    shards = [Shard(str(path)) for path in paths]
    coordinator = Coordinator(shards)
    timings = []
    try:
        with executor.PersistentPool(worker_count) as pool:
            pool.parallel(shards, QUERY, ())
            for _ in range(repetitions):
                start = time.perf_counter()
                results = pool.parallel(shards, QUERY, ())
                coordinator.merge(results, grouped=True)
                timings.append(time.perf_counter() - start)
                log(f"BENCH warm repetition {len(timings)}/{repetitions} "
                    f"({timings[-1]:.4f}s)")
    finally:
        for shard in shards:
            shard.close()
    return timings


def run_cold_repetitions(root: Path, worker_count: int, repetitions: int) -> list[float]:
    timings = []
    for repetition in range(1, repetitions + 1):
        started = time.perf_counter()
        timings.append(run_query(root, worker_count))
        log(f"BENCH cold repetition {repetition}/{repetitions} "
            f"({time.perf_counter() - started:.2f}s)")
    return timings


def write_csv(rows: list[dict[str, object]]) -> Path:
    EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
    output = EVIDENCE_ROOT / "scaling-matrix.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("rows", "shards", "workers", "mode", "repetition", "seconds"),
        )
        writer.writeheader()
        writer.writerows(rows)
    return output


def make_chart(
    rows: list[dict[str, object]],
    mode: str,
    output_name: str,
    row_series: tuple[int, ...],
    shard_series: tuple[int, ...],
    worker_series: tuple[int, ...],
) -> Path:
    """Make a normalized chart matrix with Tukey-filtered observations."""
    figure, axes = plt.subplots(
        len(row_series),
        len(shard_series),
        figsize=(18, 14),
        squeeze=False,
    )
    colors_by_rank = ("#b388ff", "#9e9e9e", "#757575", "#424242")
    all_normalized_values: list[float] = []
    subplot_data: list[
        tuple[object, list[float], list[float], list[float], list[list[float]]]
    ] = []
    rejected_total = 0
    for row_index, row_count in enumerate(row_series):
        for column_index, shard_count in enumerate(shard_series):
            axis = axes[row_index][column_index]
            selected = [
                item for item in rows
                if int(item["rows"]) == row_count
                and int(item["shards"]) == shard_count
                and item["mode"] == mode
            ]
            observations: list[list[float]] = []
            for worker_count in worker_series:
                values = [
                    float(item["seconds"]) for item in selected
                    if int(item["workers"]) == worker_count
                ]
                if not values:
                    raise RuntimeError(
                        f"missing {mode} data for {row_count} rows, "
                        f"{shard_count} shards, {worker_count} workers"
                    )
                quartile_1, quartile_3 = np.percentile(values, [25, 75])
                iqr = quartile_3 - quartile_1
                lower_bound = quartile_1 - 1.5 * iqr
                upper_bound = quartile_3 + 1.5 * iqr
                inliers = [
                    value for value in values
                    if lower_bound <= value <= upper_bound
                ]
                if not inliers:
                    raise RuntimeError(
                        f"outlier filter removed every observation for "
                        f"{row_count} rows, {shard_count} shards, "
                        f"{worker_count} workers"
                    )
                rejected_total += len(values) - len(inliers)
                observations.append(inliers)

            baseline = float(np.median(observations[0]))
            normalized_observations = [
                [value / baseline for value in values]
                for values in observations
            ]
            normalized_medians = [
                float(np.median(values)) for values in normalized_observations
            ]
            normalized_lows = [min(values) for values in normalized_observations]
            normalized_highs = [max(values) for values in normalized_observations]
            all_normalized_values.extend(normalized_highs)
            subplot_data.append(
                (
                    axis,
                    normalized_medians,
                    normalized_lows,
                    normalized_highs,
                    normalized_observations,
                )
            )

    y_max = max(all_normalized_values) * 1.12
    for axis, medians, lows, highs, observations in subplot_data:
        x = np.arange(len(worker_series))
        rank_order = sorted(range(len(medians)), key=medians.__getitem__)
        colors = [""] * len(medians)
        for rank, index in enumerate(rank_order):
            colors[index] = colors_by_rank[min(rank, len(colors_by_rank) - 1)]
        axis.bar(
            x,
            medians,
            color=colors,
            yerr=[np.array(medians) - lows, np.array(highs) - medians],
            capsize=3,
            error_kw={"elinewidth": 1, "ecolor": "#111111"},
        )
        for index, values in enumerate(observations):
            jitter = np.linspace(-0.12, 0.12, len(values))
            axis.scatter(
                np.full(len(values), x[index]) + jitter,
                values,
                color="#111111",
                s=11,
                alpha=0.7,
                zorder=3,
                linewidths=0,
            )
        axis.set_xticks(x, [str(worker) for worker in worker_series])
        axis.set_ylim(0, y_max)
        axis.grid(axis="y", alpha=0.25)
        axis.set_axisbelow(True)

    figure.suptitle(
        f"MiniShardDB {mode} grouped aggregation scaling (normalized)",
        fontsize=16,
        fontweight="bold",
    )
    figure.text(
        0.5,
        0.01,
        "1.0 = 1-worker reference; bars: median; whiskers: inlier range; "
        "points: retained repetitions; outliers rejected by 1.5xIQR",
        ha="center",
    )
    figure.text(
        0.99,
        0.01,
        f"Rejected observations: {rejected_total}",
        ha="right",
        fontsize=8,
    )
    for row_index, row_count in enumerate(row_series):
        for column_index, shard_count in enumerate(shard_series):
            axis = axes[row_index][column_index]
            axis.set_title(f"{row_count:,} rows / {shard_count} shards", fontsize=9)
            if column_index == 0:
                axis.set_ylabel("normalized execution time")
            if row_index == len(row_series) - 1:
                axis.set_xlabel("workers")
    figure.tight_layout(rect=(0, 0.03, 1, 0.95))
    FIGURES_ROOT.mkdir(parents=True, exist_ok=True)
    output = FIGURES_ROOT / output_name
    figure.savefig(output, dpi=180)
    plt.close(figure)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=parse_size_series, default=DEFAULT_ROWS)
    parser.add_argument("--shards", type=lambda value: parse_series(value, "shards"), default=DEFAULT_SHARDS)
    parser.add_argument("--workers", type=lambda value: parse_series(value, "workers"), default=DEFAULT_WORKERS)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument(
        "--generation-workers",
        type=int,
        default=min(config.DEFAULT_MAX_WORKERS, os.cpu_count() or 1),
        help="Process-pool workers used to generate each dataset",
    )
    args = parser.parse_args()
    if args.repetitions < 1 or args.batch_size < 1 or args.generation_workers < 1:
        parser.error("repetitions, batch-size, and generation-workers must be positive")

    all_rows: list[dict[str, object]] = []
    total_configurations = len(args.rows) * len(args.shards) * len(args.workers) * 2
    configuration_number = 0
    log(f"RUN start: {total_configurations} configurations, "
        f"{args.repetitions} repetitions each")
    for row_count in args.rows:
        for shard_count in args.shards:
            root = build_dataset(
                row_count,
                shard_count,
                args.batch_size,
                args.generation_workers,
            )
            for worker_count in args.workers:
                for mode in ("cold", "warm"):
                    configuration_number += 1
                    log(
                        f"BENCH start {configuration_number}/{total_configurations}: "
                        f"{row_count:,} rows / {shard_count} shards / "
                        f"{worker_count} workers / {mode}"
                    )
                    timings = (
                        run_cold_repetitions(root, worker_count, args.repetitions)
                        if mode == "cold"
                        else run_warm_repetitions(root, worker_count, args.repetitions)
                    )
                    for repetition, seconds in enumerate(timings, start=1):
                        all_rows.append(
                            {
                                "rows": row_count,
                                "shards": shard_count,
                                "workers": worker_count,
                                "mode": mode,
                                "repetition": repetition,
                                "seconds": f"{seconds:.6f}",
                            }
                        )
                    log(
                        f"{row_count:,} rows, {shard_count} shards, "
                        f"{worker_count} workers, {mode}: "
                        f"median={np.median(timings):.4f}s"
                    )

    output = write_csv(all_rows)
    log("CHART start: generating cold and warm matrices")
    make_chart(
        all_rows, "cold", "scaling-matrix-cold.png",
        args.rows, args.shards, args.workers,
    )
    make_chart(
        all_rows, "warm", "scaling-matrix-warm.png",
        args.rows, args.shards, args.workers,
    )
    log(f"RUN complete: wrote {output}")


if __name__ == "__main__":
    main()
