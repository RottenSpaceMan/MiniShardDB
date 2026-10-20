"""Benchmark shard-count and dataset-size scaling and generate two chart matrices."""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from typing import TypedDict

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config
import executor
from coordinator import Coordinator
from models import OrderRow
from shard import Shard


DEFAULT_ROWS = (100_000, 1_000_000, 5_000_000, 10_000_000)
DEFAULT_SHARDS = (2, 4, 8, 16)
DEFAULT_WORKERS = (1, 2, 4, 8)
QUERY = "SELECT status, SUM(amount) FROM orders GROUP BY status"
DATASET_ROOT = PROJECT_ROOT / "data" / "benchmark_matrix"
EVIDENCE_ROOT = PROJECT_ROOT / "report" / "evidence"
FIGURES_ROOT = PROJECT_ROOT / "report" / "figures"


class MeasurementRecord(TypedDict):
    rows: int
    shards: int
    workers: int
    mode: str
    repetition: int
    seconds: str


def reject_outliers(values: list[float]) -> tuple[list[float], int]:
    """Reject timing spikes with a robust median absolute deviation fence."""
    if not values:
        raise ValueError("cannot reject outliers from an empty sample")
    median = float(np.median(values))
    deviations = np.abs(np.asarray(values) - median)
    mad = float(np.median(deviations))
    if mad == 0:
        inliers = [value for value in values if value == median]
    else:
        robust_scores = 0.6745 * deviations / mad
        inliers = [
            value for value, score in zip(values, robust_scores)
            if score <= 3.5
        ]
    if not inliers:
        raise RuntimeError("outlier rejection removed every observation")
    return inliers, len(values) - len(inliers)


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
    """Return the isolated dataset directory for one scaling configuration."""
    return DATASET_ROOT / f"rows-{rows}" / f"shards-{shard_count}"


def order_batch(
    bounds: tuple[int, int, int],
) -> list[OrderRow]:
    """Generate one deterministic order batch in a worker process."""
    start, end, shard_count = bounds
    orders: list[OrderRow] = []
    for order_id in range(start, end):
        customer_id = ((order_id - 1) % (shard_count * 10_000)) + 1
        amount = float((order_id * 17) % 49_001 + 100) / 100
        status = config.ORDER_STATUSES[order_id % len(config.ORDER_STATUSES)]
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
        missing = [
            root / f"shard{index}.db"
            for index in range(shard_count)
            if not (root / f"shard{index}.db").is_file()
        ]
        if missing:
            raise FileNotFoundError(f"completed dataset has missing shards: {missing}")
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
            connections.append(connection)
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

        bounds = batch_bounds(rows, shard_count, batch_size)
        with ProcessPoolExecutor(max_workers=generation_workers) as pool:
            for batch_number, batch in enumerate(
                pool.map(order_batch, bounds), start=1
            ):
                grouped: dict[int, list[OrderRow]] = {}
                for order in batch:
                    grouped.setdefault(order[1] % shard_count, []).append(order)
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


def _open_shards(root: Path, resources: ExitStack) -> list[Shard]:
    """Open shards in numeric order and register their cleanup immediately."""
    paths = sorted(root.glob("shard*.db"), key=lambda path: int(path.stem[5:]))
    shards = []
    for path in paths:
        current_shard = Shard(str(path))
        resources.callback(current_shard.close)
        shards.append(current_shard)
    return shards


def run_query(root: Path, worker_count: int) -> float:
    """Measure one grouped aggregation over all shards."""
    with ExitStack() as resources:
        shards = _open_shards(root, resources)
        coordinator = Coordinator(shards)
        start = time.perf_counter()
        result = coordinator.run(QUERY, (), None, workers=worker_count)
        elapsed = time.perf_counter() - start
        if not result:
            raise RuntimeError("benchmark query returned no result")
        return elapsed


def run_warm_repetitions(root: Path, worker_count: int, repetitions: int) -> list[float]:
    """Measure persistent-pool queries after one untimed warmup."""
    timings = []
    with ExitStack() as resources:
        shards = _open_shards(root, resources)
        coordinator = Coordinator(shards)
        with executor.PersistentPool(worker_count) as pool:
            pool.parallel(shards, QUERY, ())
            for _ in range(repetitions):
                start = time.perf_counter()
                results = pool.parallel(shards, QUERY, ())
                coordinator.merge(results, grouped=True)
                timings.append(time.perf_counter() - start)
                log(f"BENCH warm repetition {len(timings)}/{repetitions} "
                    f"({timings[-1]:.4f}s)")
    return timings


def run_cold_repetitions(root: Path, worker_count: int, repetitions: int) -> list[float]:
    timings = []
    for repetition in range(1, repetitions + 1):
        started = time.perf_counter()
        timings.append(run_query(root, worker_count))
        log(f"BENCH cold repetition {repetition}/{repetitions} "
            f"({time.perf_counter() - started:.2f}s)")
    return timings


def write_csv(rows: list[MeasurementRecord]) -> Path:
    EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
    output = EVIDENCE_ROOT / "scaling-matrix.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer: csv.DictWriter[str] = csv.DictWriter(
            handle,
            fieldnames=("rows", "shards", "workers", "mode", "repetition", "seconds"),
        )
        writer.writeheader()
        writer.writerows(rows)
    return output


def read_csv(path: Path) -> list[MeasurementRecord]:
    """Read previously collected measurements for chart-only runs."""
    required_fields = {"rows", "shards", "workers", "mode", "repetition", "seconds"}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not required_fields.issubset(reader.fieldnames):
            raise RuntimeError(
                f"{path} is missing required benchmark columns: "
                f"{', '.join(sorted(required_fields))}"
            )
        rows: list[MeasurementRecord] = []
        for line_number, item in enumerate(reader, start=2):
            try:
                mode = item["mode"]
                if mode not in {"cold", "warm"}:
                    raise ValueError(f"unknown mode {mode!r}")
                rows.append(
                    {
                        "rows": int(item["rows"]),
                        "shards": int(item["shards"]),
                        "workers": int(item["workers"]),
                        "mode": mode,
                        "repetition": int(item["repetition"]),
                        "seconds": item["seconds"],
                    }
                )
            except (KeyError, TypeError, ValueError) as error:
                raise RuntimeError(
                    f"invalid benchmark record in {path} at line {line_number}"
                ) from error
    if not rows:
        raise RuntimeError(f"{path} contains no benchmark measurements")
    return rows


def generate_charts(
    rows: list[MeasurementRecord],
    row_series: tuple[int, ...],
    shard_series: tuple[int, ...],
    worker_series: tuple[int, ...],
) -> None:
    log("CHART start: generating cold and warm matrices")
    make_chart(
        rows, "cold", "scaling-matrix-cold.png",
        row_series, shard_series, worker_series,
    )
    make_chart(
        rows, "warm", "scaling-matrix-warm.png",
        row_series, shard_series, worker_series,
    )


def format_duration(seconds: float) -> str:
    """Format a timing with both millisecond and second units."""
    return f"{seconds * 1_000:.2f} ms ({seconds:.5f} s)"


def make_chart(
    rows: list[MeasurementRecord],
    mode: str,
    output_name: str,
    row_series: tuple[int, ...],
    shard_series: tuple[int, ...],
    worker_series: tuple[int, ...],
) -> Path:
    """Make a normalized chart matrix with MAD-filtered observations."""
    figure, axes = plt.subplots(
        len(row_series),
        len(shard_series),
        figsize=(18, 14),
        squeeze=False,
    )
    colors_by_rank = ("#b388ff", "#9e9e9e", "#757575", "#424242")
    all_normalized_values: list[float] = []
    subplot_data: list[tuple[Axes, list[float], list[float], list[float]]] = []
    baselines_by_axis: dict[Axes, float] = {}
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
                inliers, rejected = reject_outliers(values)
                rejected_total += rejected
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
            baselines_by_axis[axis] = baseline
            subplot_data.append(
                (
                    axis,
                    normalized_medians,
                    normalized_lows,
                    normalized_highs,
                )
            )

    y_max = max(all_normalized_values) * 1.12
    for axis, medians, lows, highs in subplot_data:
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
        f"1.0 = {worker_series[0]}-worker reference; bars: median; whiskers: inlier range; "
        "whiskers: robust 3.5-MAD inlier range; raw repetitions remain in CSV",
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
            axis.set_title(
                f"{row_count:,} rows / {shard_count} shards\n"
                f"1.0× = {format_duration(baselines_by_axis[axis])}",
                fontsize=9,
            )
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
    parser.add_argument(
        "--shards",
        type=lambda value: parse_series(value, "shards"),
        default=DEFAULT_SHARDS,
    )
    parser.add_argument(
        "--workers",
        type=lambda value: parse_series(value, "workers"),
        default=DEFAULT_WORKERS,
    )
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete the existing scaling CSV before running the benchmark",
    )
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

    output_csv = EVIDENCE_ROOT / "scaling-matrix.csv"
    if not args.fresh:
        if not output_csv.is_file():
            parser.error(
                f"{output_csv} does not exist; run with --fresh to collect measurements"
            )
        all_rows = read_csv(output_csv)
        log(f"RUN reuse: loaded {len(all_rows)} measurements from {output_csv}")
        generate_charts(all_rows, args.rows, args.shards, args.workers)
        log("RUN complete: regenerated charts from existing CSV")
        return

    if output_csv.exists():
        output_csv.unlink()
        log(f"RUN fresh: deleted {output_csv}")

    all_rows: list[MeasurementRecord] = []
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
    generate_charts(all_rows, args.rows, args.shards, args.workers)
    log(f"RUN complete: wrote {output}")


if __name__ == "__main__":
    main()
