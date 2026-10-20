"""Compare persisted routing filters with tighter, in-memory Bloom filters.

Absent FPR is measured per key/filter check, not per query. Existing-ID
precision is correct-shard hits / selected shards; recall is hits / queries.
Selection reduction is relative to broadcasting every query to every shard.
The historical sample is the first 100 central orders ordered by order_id.
Only the output CSV is written; databases, saved filters, and config are not.
"""

import argparse
import csv
import json
import pickle
import random

import sys
from contextlib import closing
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config
from bloom import Bloom
from database import connect_read_only


OUTPUT_PATH = PROJECT_ROOT / "report" / "evidence" / "routing-tuning.csv"
TARGETS = (0.001, 0.0005)


def _read_orders(path: Path) -> list[tuple[int, int]]:
    """Read existing data without creating or modifying a database."""
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Missing database: {path}")
    with closing(connect_read_only(path)) as connection:
        return connection.execute(
            "SELECT order_id, cust_id FROM orders ORDER BY order_id"
        ).fetchall()


def _load_inputs() -> tuple[list[tuple[int, int]], list[list[int]], list[Bloom]]:
    if config.NUM_SHARDS < 1:
        raise ValueError("NUM_SHARDS must be positive")
    orders = _read_orders(config.BASE_DB)
    if not orders:
        raise ValueError("Central database contains no orders")
    if any(not isinstance(value, int) for row in orders for value in row):
        raise ValueError("Order IDs and customer IDs must be integers")
    if len({order_id for order_id, _ in orders}) != len(orders):
        raise ValueError("Central order IDs must be unique")

    expected: list[list[tuple[int, int]]] = [[] for _ in range(config.NUM_SHARDS)]
    for order in orders:
        shard_id = config.shard_of(order[1])
        if not 0 <= shard_id < config.NUM_SHARDS:
            raise ValueError(f"Invalid shard {shard_id} for customer {order[1]}")
        expected[shard_id].append(order)

    current = []
    shard_ids = []
    for shard_id in range(config.NUM_SHARDS):
        path = config.shard_path(shard_id)
        filter_path = config.bloom_path(path)
        if not filter_path.is_file():
            raise FileNotFoundError(f"Missing Bloom filter: {filter_path}")
        actual = _read_orders(path)
        if actual != expected[shard_id]:
            raise ValueError(
                f"Shard {shard_id} rows do not match central data and config.shard_of"
            )
        # Like Shard, accept only trusted local pickle artifacts, never uploads.
        with filter_path.open("rb") as source:
            bloom = pickle.load(source)
        if not isinstance(bloom, Bloom):
            raise ValueError(f"Invalid Bloom filter: {filter_path}")
        if (
            bloom.bit_count < 1 or bloom.hash_count < 1
            or len(bloom.bits) != (bloom.bit_count + 7) // 8
        ):
            raise ValueError(f"Invalid Bloom layout: {filter_path}")
        current.append(bloom)
        shard_ids.append([order_id for order_id, _ in actual])
    return orders, shard_ids, current


def _routing_counts(
    filters: list[Bloom], orders: list[tuple[int, int]],
) -> tuple[int, int]:
    hits = 0
    selections = 0
    for order_id, cust_id in orders:
        actual_shard = config.shard_of(cust_id)
        for shard_id, bloom in enumerate(filters):
            if order_id in bloom:
                selections += 1
                hits += int(shard_id == actual_shard)
    return hits, selections


def _measure(
    name: str,
    target: float | None,
    filters: list[Bloom],
    sample: list[tuple[int, int]],
    historical: list[tuple[int, int]],
    maximum_id: int,
    absent_probes: int,
    seed: int,
    total_orders: int,
    shard_counts: list[int],
) -> dict:
    hits, selections = _routing_counts(filters, sample)
    historical_hits, historical_selections = _routing_counts(filters, historical)
    false_positives = sum(
        candidate in bloom
        for candidate in range(maximum_id + 1, maximum_id + absent_probes + 1)
        for bloom in filters
    )
    checks = absent_probes * len(filters)
    return {
        "configuration": name,
        "target_fpr": target,
        "seed": seed,
        "total_orders": total_orders,
        "shard_count": len(filters),
        "shard_order_counts": json.dumps(shard_counts),
        "filter_bytes": sum(len(bloom.bits) for bloom in filters),
        "total_bits": sum(bloom.bit_count for bloom in filters),
        "total_hash_count": sum(bloom.hash_count for bloom in filters),
        "bits_per_shard": json.dumps([bloom.bit_count for bloom in filters]),
        "hashes_per_shard": json.dumps([bloom.hash_count for bloom in filters]),
        "absent_probes": absent_probes,
        "absent_start_id": maximum_id + 1,
        "absent_filter_checks": checks,
        "absent_false_positives": false_positives,
        "absent_fpr": false_positives / checks,
        "existing_queries": len(sample),
        "correct_hits": hits,
        "selections": selections,
        "precision": hits / selections if selections else 0.0,
        "recall": hits / len(sample),
        "selection_reduction": 1 - selections / (len(sample) * len(filters)),
        "first100_queries": len(historical),
        "first100_correct_hits": historical_hits,
        "first100_selections": historical_selections,
        "first100_precision": (
            historical_hits / historical_selections if historical_selections else 0.0
        ),
    }


def run_experiment(
    queries: int = 10_000, absent_probes: int = 10_000, seed: int = 42,
) -> list[dict]:
    """Return three comparable measurements without writing any artifacts.

    Sample without replacement using a local RNG and a stable order-ID sort.
    Empty shards receive a minimal empty in-memory filter (capacity one).
    The current target is unknown, rather than inferred from today's config.
    """
    if queries < 1 or absent_probes < 1:
        raise ValueError("queries and absent_probes must be positive")
    orders, shard_ids, current = _load_inputs()
    if queries > len(orders):
        raise ValueError("Not enough central orders for requested queries")
    sample = random.Random(seed).sample(orders, queries)
    historical = orders[:100]
    maximum_id = orders[-1][0]
    counts = [len(ids) for ids in shard_ids]

    def measure(name: str, target: float | None, filters: list[Bloom]) -> dict:
        return _measure(
            name, target, filters, sample, historical, maximum_id,
            absent_probes, seed, len(orders), counts,
        )

    rows = [measure("current", None, current)]
    for target in TARGETS:
        filters = []
        for ids in shard_ids:
            bloom = Bloom(max(1, len(ids)), false_positive_rate=target)
            for order_id in ids:
                bloom.add(order_id)
            filters.append(bloom)
        rows.append(measure(f"in_memory_p{target}", target, filters))
    return rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", type=int, default=10_000)
    parser.add_argument("--absent-probes", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    if args.queries < 1 or args.absent_probes < 1:
        parser.error("--queries and --absent-probes must be positive")
    rows = run_experiment(args.queries, args.absent_probes, args.seed)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(json.dumps(row, sort_keys=True))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
