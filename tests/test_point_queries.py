"""Checks for indexed point-query evidence collection."""

import json
import sqlite3
from contextlib import closing
from unittest.mock import Mock

import pytest

import config
import executor
from coordinator import Coordinator
from experiments import verify_point_queries as benchmark
from experiments.verify_point_queries import (
    PointQueryRunner,
    measure_pages,
    sample_queries,
    summarize,
)
from shard import Shard


def test_sampling_uses_real_ids_and_guaranteed_misses():
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE orders(order_id INTEGER PRIMARY KEY)")
        connection.executemany("INSERT INTO orders VALUES (?)", [(2,), (10,), (50,)])
        queries = sample_queries(connection, 3, 42)
        assert queries == sample_queries(connection, 3, 42)
        assert {key for kind, key in queries if kind == "hit"} == {2, 10, 50}
        assert [key for kind, key in queries if kind == "miss"] == [51, 52, 53]
        with pytest.raises(ValueError):
            sample_queries(connection, 4, 42)


def test_page_measurements_include_all_selected_databases(tmp_path):
    paths = [tmp_path / f"shard{i}.db" for i in range(2)]
    for index, path in enumerate(paths):
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE orders(order_id INTEGER PRIMARY KEY)")
            connection.execute("INSERT INTO orders VALUES (?)", (index,))
    rows, hits, misses = measure_pages(paths, 1, False)
    assert rows == [(1,)]
    assert hits >= 0
    assert misses > 0
    assert measure_pages(paths, 1, True)[2] == 0
    assert measure_pages([], 100, False) == ([], 0, 0)


def test_summary_keeps_workloads_and_cache_modes_separate():
    timings = [
        {"workload": "hit", "system": "central", "seconds": value}
        for value in (0.001, 0.003)
    ]
    pages = [{
        "workload": "hit", "system": "central", "cache": "fresh_connection",
        "selected_databases": 1, "cache_hits": 1, "cache_misses": 2,
    }]
    result = summarize(timings, pages)
    assert result["latency"][0]["median_ms"] == 2
    assert result["latency"][0]["p95_ms"] == 3
    assert result["pages"][0]["cache_misses"] == 2


@pytest.fixture
def runner():
    central = Mock()
    central.execute.return_value.fetchall.return_value = [(123,)]
    coordinator = Mock()
    coordinator.shards = [Mock(), Mock()]
    coordinator.route.return_value = coordinator.shards[:1]
    coordinator.run.return_value = [(123,)]
    coordinator.merge.side_effect = Coordinator([]).merge
    pool = Mock()
    pool.parallel.return_value = [[(123,)]]
    return PointQueryRunner(central, coordinator, pool, workers=4)


def test_runner_uses_central_connection(runner):
    assert runner.execute("central_reused", 123) == [(123,)]
    runner.central.execute.assert_called_once_with(benchmark.SQL, (123,))
    runner.pool.parallel.assert_not_called()


def test_runner_delegates_direct_path_to_coordinator(runner):
    assert runner.execute("bloom_direct", 123) == [(123,)]
    runner.coordinator.run.assert_called_once_with(
        benchmark.SQL, (123,), ("order_id", 123), workers=4
    )
    runner.pool.parallel.assert_not_called()


@pytest.mark.parametrize("system", ["bloom_warm_pool", "broadcast_warm_pool"])
def test_runner_keeps_routed_and_broadcast_baselines_separate(runner, system):
    assert runner.execute(system, 123) == [(123,)]
    if system == "bloom_warm_pool":
        runner.coordinator.route.assert_called_once_with(("order_id", 123))
        targets = runner.coordinator.route.return_value
    else:
        runner.coordinator.route.assert_not_called()
        targets = runner.coordinator.shards
    runner.pool.parallel.assert_called_once_with(targets, benchmark.SQL, (123,))
    runner.coordinator.run.assert_not_called()


def test_runner_uses_legacy_executor_for_cold_baseline(runner, monkeypatch):
    parallel = Mock(return_value=[[(123,)]])
    monkeypatch.setattr(executor, "parallel", parallel)
    assert runner.execute("bloom_cold_pool", 123) == [(123,)]
    parallel.assert_called_once_with(
        runner.coordinator.route.return_value, benchmark.SQL, (123,), workers=4
    )
    runner.coordinator.run.assert_not_called()
    runner.pool.parallel.assert_not_called()


def test_runner_rejects_unknown_system_instead_of_broadcasting(runner):
    with pytest.raises(ValueError, match="unknown benchmark system"):
        runner.execute("typo", 123)
    runner.central.execute.assert_not_called()
    runner.pool.parallel.assert_not_called()
    runner.coordinator.route.assert_not_called()


@pytest.mark.parametrize("arguments", [
    ["--queries", "0"],
    ["--repetitions", "0"],
    ["--workers", "0"],
    ["--cold-queries", "-1"],
    ["--output-prefix", "nested/output"],
    ["--output-prefix", ""],
])
def test_invalid_arguments_fail_before_running_benchmark(arguments):
    with pytest.raises(SystemExit) as error:
        benchmark.parse_args(arguments)
    assert error.value.code == 2


def test_query_plans_close_their_connections(tmp_path, monkeypatch):
    path = tmp_path / "base.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE orders(order_id INTEGER PRIMARY KEY)")
    monkeypatch.setattr(benchmark, "PROJECT_ROOT", tmp_path)
    plans = benchmark.query_plans([path], 123)
    detail = plans["base.db"][0][3]
    assert isinstance(detail, str)
    assert "USING INTEGER PRIMARY KEY" in detail
    path.unlink()


def test_main_writes_all_comparisons_without_overwriting_other_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BASE_DB", tmp_path / "base.db")
    monkeypatch.setattr(config, "SHARD_DIR", tmp_path)
    monkeypatch.setattr(config, "NUM_SHARDS", 2)
    monkeypatch.setattr(benchmark, "PROJECT_ROOT", tmp_path)
    orders = [
        (122, 0, 10.0, "2026-01-01", "pending"),
        (123, 1, 20.0, "2026-01-01", "pending"),
    ]
    for index, path in enumerate([config.BASE_DB, config.shard_path(0), config.shard_path(1)]):
        selected = orders if index == 0 else [row for row in orders if row[1] == index - 1]
        with Shard(str(path)) as shard:
            shard.create()
            shard.insert_customers([
                (row[1], "Customer", "customer@example.com") for row in selected
            ])
            shard.insert_orders(selected)
            shard.build_bloom()

    class LocalPool:
        def __init__(self, workers):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def parallel(self, targets, sql, params):
            return [shard.execute(sql, params) for shard in targets]

    monkeypatch.setattr(executor, "PersistentPool", LocalPool)
    evidence = tmp_path / "report" / "evidence"
    evidence.mkdir(parents=True)
    saved = evidence / "point-query-summary.json"
    saved.write_text("existing evidence", encoding="utf-8")
    benchmark.main([
        "--queries", "1", "--repetitions", "1", "--cold-queries", "0",
        "--workers", "1", "--output-prefix", "cleanup-smoke",
    ])
    summary = json.loads((evidence / "cleanup-smoke-summary.json").read_text())
    assert len(summary["latency"]) == 2 * len(benchmark.WARM_SYSTEMS)
    assert {row["system"] for row in summary["latency"]} == set(benchmark.WARM_SYSTEMS)
    assert len(summary["pages"]) == 12
    assert saved.read_text() == "existing evidence"
    manifest = json.loads((evidence / "cleanup-smoke-manifest.json").read_text())
    assert manifest["arguments"]["output_prefix"] == "cleanup-smoke"
    assert len(manifest["plans"]) == 3
