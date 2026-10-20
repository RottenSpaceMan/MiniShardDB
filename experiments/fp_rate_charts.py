"""Measure Bloom false positives and routing precision, then plot E1 results."""

import csv
import math
import sys
from collections.abc import Mapping, Sequence
from contextlib import ExitStack, closing
from pathlib import Path
from typing import TypedDict

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config
from coordinator import Coordinator
from database import connect_read_only
from shard import Shard

BASE_DB: Path = config.BASE_DB
EVIDENCE_DIR: Path = PROJECT_ROOT / "report" / "evidence"
FIGURES_DIR: Path = PROJECT_ROOT / "report" / "figures"
NUM_SHARDS: int = config.NUM_SHARDS
DEFAULT_PROBES: int = 1_000_000
DEFAULT_ROUTING_QUERIES: int = 100
TARGET_FALSE_POSITIVE_RATE: float = config.BLOOM_FALSE_POSITIVE_RATE


class FalsePositiveRow(TypedDict):
    shard: str
    probe: int
    false_positive: int
    cumulative_false_positive_rate: float
    theoretical_rate: float


class RoutingRow(TypedDict):
    query: int
    order_id: int
    actual_shard: int
    routed_shards: int
    actual_shard_found: int
    unnecessary_shards: int


def theoretical_rate(item_count: int, bit_count: int, hash_count: int) -> float:
    """Return the expected Bloom false-positive probability."""
    return (1 - math.exp(-hash_count * item_count / bit_count)) ** hash_count


def measure_false_positives(
    probes: int,
) -> list[FalsePositiveRow]:
    """Probe every Bloom filter with keys verified absent from every shard."""
    if probes < 1:
        raise ValueError("probes must be positive")

    with closing(connect_read_only(BASE_DB)) as connection:
        maximum_order_id = connection.execute(
            "SELECT MAX(order_id) FROM orders"
        ).fetchone()[0]
    if not isinstance(maximum_order_id, int):
        raise RuntimeError("base database contains no order IDs")

    rows: list[FalsePositiveRow] = []
    aggregate_by_probe: list[int] = [0] * probes

    for shard_id in range(NUM_SHARDS):
        shard = Shard(str(config.shard_path(shard_id)))
        try:
            if shard.bloom is None:
                raise RuntimeError(f"shard {shard_id} has no Bloom filter")
            item_count = int(
                shard.con.execute("SELECT COUNT(order_id) FROM orders").fetchone()[0]
            )
            expected_rate = theoretical_rate(
                item_count, shard.bloom.bit_count, shard.bloom.hash_count
            )
            shard_false_positives = 0
            for probe_number in range(1, probes + 1):
                candidate = maximum_order_id + probe_number
                is_false_positive = int(shard.bloom_has(candidate))
                shard_false_positives += is_false_positive
                aggregate_by_probe[probe_number - 1] += is_false_positive
                rows.append(
                    {
                        "shard": str(shard_id),
                        "probe": probe_number,
                        "false_positive": is_false_positive,
                        "cumulative_false_positive_rate": (
                            shard_false_positives / probe_number
                        ),
                        "theoretical_rate": expected_rate,
                    }
                )
        finally:
            shard.close()

    running_false_positives = 0
    for probe_number, false_positives in enumerate(aggregate_by_probe, start=1):
        running_false_positives += false_positives
        rows.append(
            {
                "shard": "all",
                "probe": probe_number,
                "false_positive": false_positives,
                "cumulative_false_positive_rate": (
                    running_false_positives / (probe_number * NUM_SHARDS)
                ),
                "theoretical_rate": TARGET_FALSE_POSITIVE_RATE,
            }
        )
    return rows


def measure_routing_precision(
    query_count: int,
) -> list[RoutingRow]:
    """Measure Bloom routing decisions for real order IDs."""
    if query_count < 1:
        raise ValueError("query_count must be positive")

    with ExitStack() as resources:
        shards = []
        for shard_id in range(NUM_SHARDS):
            current_shard = Shard(str(config.shard_path(shard_id)))
            resources.callback(current_shard.close)
            shards.append(current_shard)
        with closing(connect_read_only(BASE_DB)) as connection:
            orders = connection.execute(
                "SELECT order_id, cust_id FROM orders ORDER BY order_id LIMIT ?",
                (query_count,),
            ).fetchall()
        if len(orders) < query_count:
            raise ValueError("not enough orders for requested routing queries")

        coordinator = Coordinator(shards)
        rows: list[RoutingRow] = []
        for query_number, (order_id, cust_id) in enumerate(orders, start=1):
            actual_shard = config.shard_of(int(cust_id))
            routed = coordinator.route(("order_id", int(order_id)))
            routed_ids = {
                shard_id
                for shard_id, shard in enumerate(shards)
                if any(routed_shard is shard for routed_shard in routed)
            }
            rows.append(
                {
                    "query": query_number,
                    "order_id": int(order_id),
                    "actual_shard": actual_shard,
                    "routed_shards": len(routed),
                    "actual_shard_found": int(actual_shard in routed_ids),
                    "unnecessary_shards": len(routed_ids - {actual_shard}),
                }
            )
        return rows


def write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    """Write experiment rows using their dictionary keys as CSV columns."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError("cannot write an empty result set")
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def create_plot(rows: list[FalsePositiveRow]) -> Path:
    """Create the per-shard and aggregate cumulative-rate figure."""
    figure, axis = plt.subplots(figsize=(9, 5.5))
    max_probe = max(row["probe"] for row in rows if row["shard"] != "all")
    for shard_id in range(NUM_SHARDS):
        shard_rows = [row for row in rows if row["shard"] == str(shard_id)]
        axis.plot(
            [row["probe"] for row in shard_rows],
            [row["cumulative_false_positive_rate"] for row in shard_rows],
            linewidth=1,
            alpha=0.6,
            label=f"Shard {shard_id}",
        )

    aggregate_rows = [row for row in rows if row["shard"] == "all"]
    aggregate_rate = aggregate_rows[-1]["cumulative_false_positive_rate"]
    axis.plot(
        [row["probe"] for row in aggregate_rows],
        [row["cumulative_false_positive_rate"] for row in aggregate_rows],
        color="#d62728",
        linewidth=2.5,
        label=f"Aggregate: {aggregate_rate:.2%}",
    )
    axis.axhline(
        TARGET_FALSE_POSITIVE_RATE,
        color="black",
        linestyle="--",
        linewidth=1.5,
        label=f"Configured target ({TARGET_FALSE_POSITIVE_RATE:.2%})",
    )
    axis.set_title("E1: Bloom-filter cumulative false-positive rate")
    axis.set_xlabel("Verified absent-key probes per shard")
    axis.set_ylabel("Cumulative false-positive rate")
    axis.set_ylim(bottom=0)
    axis.set_xlim(0, max_probe)
    axis.yaxis.set_major_formatter(
        FuncFormatter(lambda value, _: f"{value:.2%}")
    )
    axis.grid(axis="y", alpha=0.3)
    axis.legend(ncol=2, fontsize=8)

    early_probe_limit = min(500, max_probe)
    inset = inset_axes(axis, width="34%", height="38%", loc="upper right")
    for shard_id in range(NUM_SHARDS):
        shard_rows = [
            row
            for row in rows
            if row["shard"] == str(shard_id)
            and row["probe"] <= early_probe_limit
        ]
        inset.plot(
            [row["probe"] for row in shard_rows],
            [row["cumulative_false_positive_rate"] for row in shard_rows],
            linewidth=0.7,
            alpha=0.6,
        )
    early_aggregate = [
        row
        for row in aggregate_rows
        if row["probe"] <= early_probe_limit
    ]
    inset.plot(
        [row["probe"] for row in early_aggregate],
        [row["cumulative_false_positive_rate"] for row in early_aggregate],
        color="#d62728",
        linewidth=1.5,
    )
    inset.axhline(TARGET_FALSE_POSITIVE_RATE, color="black", linestyle="--")
    inset.set_xlim(1, early_probe_limit)
    inset.set_ylim(bottom=0)
    inset.set_title("First probes", fontsize=8)
    inset.tick_params(labelsize=7)
    inset.grid(axis="y", alpha=0.2)

    figure.tight_layout()

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    output_path = FIGURES_DIR / "e1-bloom-fp-rate.png"
    figure.savefig(output_path, dpi=180)
    plt.close(figure)
    return output_path


def main() -> None:
    """Run E1 and write CSV evidence plus the false-positive figure."""
    if not BASE_DB.exists():
        raise FileNotFoundError(f"missing database: {BASE_DB}")
    missing_shards = [
        str(path)
        for shard_id in range(NUM_SHARDS)
        if not (path := config.shard_path(shard_id)).exists()
    ]
    if missing_shards:
        raise FileNotFoundError("missing shard databases: " + ", ".join(missing_shards))

    false_positive_rows = measure_false_positives(DEFAULT_PROBES)
    routing_rows = measure_routing_precision(DEFAULT_ROUTING_QUERIES)
    write_csv(
        EVIDENCE_DIR / "fp-rate.csv",
        false_positive_rows,
    )
    write_csv(
        EVIDENCE_DIR / "routing-precision.csv",
        routing_rows,
    )
    figure_path = create_plot(false_positive_rows)

    aggregate = false_positive_rows[-1]
    routing_recall = sum(row["actual_shard_found"] for row in routing_rows)
    unnecessary = sum(row["unnecessary_shards"] for row in routing_rows)
    routed_shards = sum(row["routed_shards"] for row in routing_rows)
    routing_precision = routing_recall / routed_shards if routed_shards else 0.0
    print(
        "Aggregate false-positive rate: "
        f"{aggregate['cumulative_false_positive_rate']:.2%}"
    )
    print(
        "Routing recall: "
        f"{routing_recall / len(routing_rows):.2%}"
    )
    print(f"Routing precision: {routing_precision:.2%}")
    print(f"Unnecessary routed shards: {unnecessary}")
    print(f"Wrote {EVIDENCE_DIR / 'fp-rate.csv'}")
    print(f"Wrote {EVIDENCE_DIR / 'routing-precision.csv'}")
    print(f"Wrote {figure_path}")


if __name__ == "__main__":
    main()
