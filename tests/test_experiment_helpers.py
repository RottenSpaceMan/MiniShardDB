"""Small experiment regressions that never use project data or evidence."""

import argparse
import csv
import math
import sqlite3
from contextlib import ExitStack, closing

import pytest

import config
from experiments import benchmark_matrix, verify_perf, verify_security


@pytest.mark.parametrize("text", ["", "1,", "x", "0", "-1,2"])
def test_positive_series_reject_invalid_values(text):
    with pytest.raises(argparse.ArgumentTypeError):
        benchmark_matrix.parse_series(text, "workers")
    with pytest.raises(argparse.ArgumentTypeError):
        verify_perf.parse_worker_series(text)


def test_series_preserve_order_duplicates_and_suffixes():
    assert benchmark_matrix.parse_series(" 4, 1,4 ", "workers") == (4, 1, 4)
    assert verify_perf.parse_worker_series(" 4, 1,4 ") == [4, 1, 4]
    assert benchmark_matrix.parse_size_series("2K, 1m, 3") == (2000, 1_000_000, 3)


@pytest.mark.parametrize("text", ["", "1.5k", "0", "-1m", "2k,"])
def test_size_series_reject_invalid_values(text):
    with pytest.raises(argparse.ArgumentTypeError):
        benchmark_matrix.parse_size_series(text)


def test_order_batches_preserve_bounds_and_generated_values():
    bounds = benchmark_matrix.batch_bounds(7, 2, 3)
    assert bounds == [(1, 4, 2), (4, 7, 2), (7, 8, 2)]
    orders = [order for bound in bounds for order in benchmark_matrix.order_batch(bound)]
    assert [row[0] for row in orders] == list(range(1, 8))
    assert orders[0] == (1, 1, 1.17, "2026-01-01", config.ORDER_STATUSES[1])
    assert benchmark_matrix.order_batch((20_000, 20_002, 2)) == [
        (20_000, 20_000, 460.94, "2026-01-01", config.ORDER_STATUSES[0]),
        (20_001, 1, 461.11, "2026-01-01", config.ORDER_STATUSES[1]),
    ]


@pytest.mark.parametrize(
    ("values", "expected"),
    [([2.0], ([2.0], 0)), ([1.0, 1.0, 9.0], ([1.0, 1.0], 1)),
     ([1.0, 2.0, 3.0, 100.0], ([1.0, 2.0, 3.0], 1))],
)
def test_outlier_filter_preserves_mad_formula(values, expected):
    assert benchmark_matrix.reject_outliers(values) == expected


def test_outlier_filter_rejects_empty_sample():
    with pytest.raises(ValueError, match="empty sample"):
        benchmark_matrix.reject_outliers([])


def test_measurement_csv_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark_matrix, "EVIDENCE_ROOT", tmp_path)
    rows = [
        benchmark_matrix.MeasurementRecord(
            rows=10, shards=2, workers=1, mode="cold", repetition=1, seconds="0.001234"
        )
    ]
    path = benchmark_matrix.write_csv(rows)
    assert path.name == "scaling-matrix.csv"
    assert benchmark_matrix.read_csv(path) == rows
    assert path.read_text().splitlines()[0] == "rows,shards,workers,mode,repetition,seconds"


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("rows,seconds\n1,0.1\n", "missing required benchmark columns"),
        ("rows,shards,workers,mode,repetition,seconds\n", "no benchmark measurements"),
        ("rows,shards,workers,mode,repetition,seconds\n1,2,1,other,1,0.1\n", "line 2"),
        ("rows,shards,workers,mode,repetition,seconds\nx,2,1,cold,1,0.1\n", "line 2"),
    ],
)
def test_measurement_csv_validation(tmp_path, content, message):
    path = tmp_path / "measurements.csv"
    path.write_text(content)
    with pytest.raises(RuntimeError, match=message):
        benchmark_matrix.read_csv(path)


def test_open_shards_uses_numeric_order_and_closes_on_failure(tmp_path, monkeypatch):
    for name in ("shard10.db", "shard2.db", "shard1.db"):
        (tmp_path / name).touch()
    events = []

    class FakeShard:
        def __init__(self, path):
            self.path = path
            events.append(("open", path))

        def close(self):
            events.append(("close", self.path))

    monkeypatch.setattr(benchmark_matrix, "Shard", FakeShard)
    with pytest.raises(RuntimeError, match="query failed"):
        with ExitStack() as resources:
            shards = benchmark_matrix._open_shards(tmp_path, resources)
            assert [shard.path for shard in shards] == [
                str(tmp_path / f"shard{index}.db") for index in (1, 2, 10)
            ]
            raise RuntimeError("query failed")
    assert events[3:] == [("close", path) for _, path in reversed(events[:3])]


def test_timed_returns_seconds_before_result(monkeypatch):
    ticks = iter([10.0, 10.25])
    monkeypatch.setattr(verify_perf.time, "perf_counter", lambda: next(ticks))
    assert verify_perf.timed(lambda: "result") == (0.25, "result")


def test_grouped_results_preserve_float_tolerance():
    assert verify_perf.results_match([("pending", 1.0)], {"pending": 1.0 + 1e-13}, True)
    assert not verify_perf.results_match([("pending", 1.0)], {"pending": 1.01}, True)
    assert not verify_perf.results_match([("pending", 1.0)], {"shipped": 1.0}, True)
    assert verify_perf.results_match([(1,)], [(1,)], False)
    assert not verify_perf.results_match([(1,)], [(2,)], False)
    with pytest.raises(TypeError, match="unsupported result type"):
        verify_perf.comparable_result("invalid", True)


def test_append_measurement_preserves_schema_and_precision():
    rows = []
    verify_perf.append_rows(rows, "MiniShardDB", "warm", 2, 3, 0.1234567)
    assert rows == [{
        "system": "MiniShardDB", "mode": "warm", "workers": 2,
        "repetition": 3, "seconds": "0.123457",
    }]


def test_centralized_queries_use_temporary_database(tmp_path):
    path = tmp_path / "central.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE orders (amount REAL)")
        connection.executemany("INSERT INTO orders VALUES (?)", [(1.0,), (2.0,)])
        seconds, rows = verify_perf.run_centralized(connection, "SELECT SUM(amount) FROM orders")
        assert seconds >= 0
        assert rows == [(3.0,)]
    seconds, rows = verify_perf.run_centralized_cold(path, "SELECT SUM(amount) FROM orders")
    assert seconds >= 0
    assert rows == [(3.0,)]


@pytest.mark.parametrize("payload", verify_security.PAYLOADS)
def test_security_payload_results_remain_unchanged(payload):
    assert verify_security.vulnerable_result(payload) == "accepted"
    assert verify_security.safe_result(payload) == "blocked"


def test_read_only_shard_verification_counts_misplaced_rows(tmp_path, monkeypatch):
    from experiments import verify_shards

    monkeypatch.setattr(config, "NUM_SHARDS", 2)
    monkeypatch.setattr(config, "SHARD_DIR", tmp_path)
    for shard_id in range(2):
        with closing(sqlite3.connect(config.shard_path(shard_id))) as connection, connection:
            connection.execute("CREATE TABLE orders (cust_id INTEGER)")
            connection.execute("CREATE TABLE customers (cust_id INTEGER)")
            connection.executemany("INSERT INTO orders VALUES (?)", [(0,), (1,), (2,)])
            connection.executemany("INSERT INTO customers VALUES (?)", [(0,), (1,)])
    before = {path: path.read_bytes() for path in tmp_path.glob("*.db")}
    assert verify_shards.verify_shards() == [
        {"shard": 0, "orders": 3, "customers": 2, "wrong_orders": 1, "wrong_customers": 1},
        {"shard": 1, "orders": 3, "customers": 2, "wrong_orders": 2, "wrong_customers": 1},
    ]
    assert before == {path: path.read_bytes() for path in before}


def test_read_only_false_positive_rows_and_csv(tmp_path, monkeypatch):
    from experiments import fp_rate_charts

    base = tmp_path / "base.db"
    with closing(sqlite3.connect(base)) as connection, connection:
        connection.execute("CREATE TABLE orders (order_id INTEGER)")
        connection.execute("INSERT INTO orders VALUES (10)")
    monkeypatch.setattr(fp_rate_charts, "BASE_DB", base)
    monkeypatch.setattr(fp_rate_charts, "NUM_SHARDS", 2)
    closed = []
    candidates = []

    class FakeBloom:
        bit_count = 16
        hash_count = 2

    class FakeShard:
        bloom = FakeBloom()

        def __init__(self, path):
            self.path = path
            self.con = sqlite3.connect(":memory:")
            self.con.execute("CREATE TABLE orders (order_id INTEGER)")
            self.con.execute("INSERT INTO orders VALUES (1)")

        def bloom_has(self, candidate):
            candidates.append(candidate)
            return candidate == 12

        def close(self):
            self.con.close()
            closed.append(self.path)

    monkeypatch.setattr(fp_rate_charts, "Shard", FakeShard)
    rows = fp_rate_charts.measure_false_positives(3)
    assert candidates == [11, 12, 13, 11, 12, 13]
    assert len(closed) == 2
    assert [row["shard"] for row in rows] == ["0"] * 3 + ["1"] * 3 + ["all"] * 3
    assert [row["cumulative_false_positive_rate"] for row in rows] == [0, 1 / 2, 1 / 3] * 3
    assert [row["false_positive"] for row in rows] == [0, 1, 0, 0, 1, 0, 0, 2, 0]
    assert rows[0]["theoretical_rate"] == (1 - math.exp(-2 / 16)) ** 2
    assert rows[-1]["theoretical_rate"] == config.BLOOM_FALSE_POSITIVE_RATE
    path = tmp_path / "evidence" / "fp.csv"
    fp_rate_charts.write_csv(path, rows)
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == list(rows[0])
        assert len(list(reader)) == 9
    with pytest.raises(ValueError, match="empty result set"):
        fp_rate_charts.write_csv(tmp_path / "empty.csv", [])
