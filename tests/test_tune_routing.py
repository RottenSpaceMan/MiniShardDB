import csv
import json
import pickle
import random
import runpy
import sqlite3
from contextlib import closing

import pytest

import config
from bloom import Bloom
from experiments import tune_routing


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "NUM_SHARDS", 3)
    monkeypatch.setattr(config, "BASE_DB", tmp_path / "central.db")
    monkeypatch.setattr(config, "SHARD_DIR", tmp_path / "shards")
    monkeypatch.setattr(tune_routing, "OUTPUT_PATH", tmp_path / "evidence" / "tuning.csv")
    config.SHARD_DIR.mkdir()
    # Sparse, nonconsecutive IDs ensure sampling uses actual rows, not an ID range.
    orders = [(11 + i * 13, 100 + i * 7) for i in range(120)]
    paths = [config.BASE_DB] + [config.shard_path(i) for i in range(3)]
    for index, path in enumerate(paths):
        selected = orders if index == 0 else [
            row for row in orders if config.shard_of(row[1]) == index - 1
        ]
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, cust_id INTEGER)"
            )
            connection.executemany("INSERT INTO orders VALUES (?, ?)", reversed(selected))
        if index:
            bloom = Bloom(len(selected), false_positive_rate=0.15)
            for order_id, _ in selected:
                bloom.add(order_id)
            config.bloom_path(path).write_bytes(pickle.dumps(bloom))
    return orders


def test_metrics_match_independent_counts_and_preserve_inputs(dataset):
    input_paths = [config.BASE_DB] + [config.shard_path(i) for i in range(3)]
    input_paths += [config.bloom_path(config.shard_path(i)) for i in range(3)]
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in input_paths}
    rows = tune_routing.run_experiment(queries=77, absent_probes=200, seed=19)
    assert [row["target_fpr"] for row in rows] == [None, 0.001, 0.0005]
    assert rows[0]["configuration"] == "current"
    sample = random.Random(19).sample(dataset, 77)
    for row in rows:
        filters = []
        for shard_id in range(3):
            if row["target_fpr"] is None:
                bloom = pickle.loads(config.bloom_path(config.shard_path(shard_id)).read_bytes())
            else:
                ids = [oid for oid, cid in dataset if config.shard_of(cid) == shard_id]
                bloom = Bloom(len(ids), row["target_fpr"])
                for order_id in ids:
                    bloom.add(order_id)
            filters.append(bloom)
        selections = sum(oid in bloom for oid, _ in sample for bloom in filters)
        correct = sum(oid in filters[config.shard_of(cid)] for oid, cid in sample)
        first_selections = sum(oid in bloom for oid, _ in dataset[:100] for bloom in filters)
        false_positives = sum(
            oid in bloom
            for oid in range(dataset[-1][0] + 1, dataset[-1][0] + 201)
            for bloom in filters
        )
        assert row["total_orders"] == 120
        assert row["existing_queries"] == 77
        assert row["correct_hits"] == correct == 77
        assert row["selections"] == selections
        assert row["precision"] == correct / selections
        assert row["recall"] == 1.0
        assert row["selection_reduction"] == 1 - selections / (77 * 3)
        assert row["first100_queries"] == 100
        assert row["first100_correct_hits"] == 100
        assert row["first100_selections"] == first_selections
        assert row["first100_precision"] == 100 / first_selections
        assert row["absent_start_id"] == dataset[-1][0] + 1
        assert row["absent_filter_checks"] == 600
        assert row["absent_false_positives"] == false_positives
        assert row["absent_fpr"] == false_positives / 600
        assert row["filter_bytes"] == sum(len(b.bits) for b in filters)
        assert row["total_bits"] == sum(b.bit_count for b in filters)
        assert row["total_hash_count"] == sum(b.hash_count for b in filters)
        assert json.loads(row["hashes_per_shard"]) == [b.hash_count for b in filters]
        assert json.loads(row["bits_per_shard"]) == [b.bit_count for b in filters]
        assert json.loads(row["shard_order_counts"]) == [40, 40, 40]
    assert before == {
        path: (path.read_bytes(), path.stat().st_mtime_ns) for path in input_paths
    }
    assert config.NUM_SHARDS == 3
    assert not tune_routing.OUTPUT_PATH.exists()


def test_sampling_is_reproducible_and_historical_sample_is_separate(dataset, monkeypatch):
    samples = []
    original = tune_routing._routing_counts

    def record(filters, orders):
        samples.append(orders.copy())
        return original(filters, orders)

    monkeypatch.setattr(tune_routing, "_routing_counts", record)
    rng_state = random.getstate()
    first = tune_routing.run_experiment(17, 20, 42)
    assert random.getstate() == rng_state
    assert all(samples[i] == random.Random(42).sample(dataset, 17) for i in (0, 2, 4))
    assert all(samples[i] == dataset[:100] for i in (1, 3, 5))
    assert first == tune_routing.run_experiment(17, 20, 42)
    tune_routing.run_experiment(17, 20, 43)
    assert samples[-2] != samples[0]


def test_stale_filter_recall_is_reported_not_assumed(dataset):
    path = config.bloom_path(config.shard_path(0))
    path.write_bytes(pickle.dumps(Bloom(40)))
    rows = tune_routing.run_experiment(120, 10)
    assert rows[0]["correct_hits"] == 80
    assert rows[0]["recall"] == 80 / 120
    assert all(row["recall"] == 1 for row in rows[1:])


def test_small_dataset_and_empty_shard(dataset):
    for path in [config.BASE_DB] + [config.shard_path(i) for i in range(3)]:
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute("DELETE FROM orders WHERE order_id NOT IN (11, 24)")
            remaining = connection.execute("SELECT order_id FROM orders").fetchall()
        if path != config.BASE_DB:
            bloom = Bloom(max(1, len(remaining)))
            for (order_id,) in remaining:
                bloom.add(order_id)
            config.bloom_path(path).write_bytes(pickle.dumps(bloom))
    rows = tune_routing.run_experiment(2, 10)
    assert all(row["first100_queries"] == 2 for row in rows)
    assert all(row["correct_hits"] == 2 for row in rows)
    assert all(row["recall"] == 1.0 for row in rows)
    assert 0 in json.loads(rows[0]["shard_order_counts"])


def test_empty_central_database_is_rejected(dataset):
    with closing(sqlite3.connect(config.BASE_DB)) as connection, connection:
        connection.execute("DELETE FROM orders")
    with pytest.raises(ValueError, match="contains no orders"):
        tune_routing.run_experiment(1, 10)


def test_zero_selections_have_zero_precision(dataset):
    for shard_id in range(3):
        config.bloom_path(config.shard_path(shard_id)).write_bytes(pickle.dumps(Bloom(40)))
    row = tune_routing.run_experiment(10, 10)[0]
    assert row["selections"] == row["correct_hits"] == 0
    assert row["precision"] == row["recall"] == row["first100_precision"] == 0.0
    assert row["selection_reduction"] == 1.0


def test_missing_filter_fails_without_rebuilding(dataset):
    path = config.bloom_path(config.shard_path(1))
    path.unlink()
    with pytest.raises(FileNotFoundError, match="Missing Bloom filter"):
        tune_routing.run_experiment(10, 10)
    assert not path.exists()
    assert not tune_routing.OUTPUT_PATH.exists()


@pytest.mark.parametrize("invalid", [None, "not a filter"])
def test_invalid_filter_fails(dataset, invalid):
    config.bloom_path(config.shard_path(0)).write_bytes(pickle.dumps(invalid))
    with pytest.raises(ValueError, match="Invalid Bloom filter"):
        tune_routing.run_experiment(10, 10)


def test_invalid_layout_fails(dataset):
    bloom = Bloom(40)
    bloom.bits = bytearray()
    config.bloom_path(config.shard_path(0)).write_bytes(pickle.dumps(bloom))
    with pytest.raises(ValueError, match="Invalid Bloom layout"):
        tune_routing.run_experiment(10, 10)


@pytest.mark.parametrize("sql", [
    "DELETE FROM orders WHERE order_id = (SELECT min(order_id) FROM orders)",
    "UPDATE orders SET cust_id = cust_id + 1",
    "INSERT INTO orders VALUES (999999, 0)",
])
def test_shard_mismatch_is_rejected(dataset, sql):
    with closing(sqlite3.connect(config.shard_path(0))) as connection, connection:
        connection.execute(sql)
    with pytest.raises(ValueError, match="Shard 0 rows do not match"):
        tune_routing.run_experiment(10, 10)


@pytest.mark.parametrize("missing", ["central", "shard"])
def test_missing_database_is_not_created(dataset, missing):
    path = config.BASE_DB if missing == "central" else config.shard_path(0)
    path.unlink()
    with pytest.raises(FileNotFoundError, match="Missing database"):
        tune_routing.run_experiment(10, 10)
    assert not path.exists()


@pytest.mark.parametrize(("queries", "probes"), [(0, 10), (-1, 10), (10, 0), (10, -1)])
def test_positive_counts_required(queries, probes):
    with pytest.raises(ValueError, match="must be positive"):
        tune_routing.run_experiment(queries, probes)


def test_excess_queries_rejected(dataset):
    with pytest.raises(ValueError, match="Not enough"):
        tune_routing.run_experiment(121, 10)


def test_main_writes_only_csv(dataset, capsys):
    tune_routing.main(["--queries", "37", "--absent-probes", "55", "--seed", "7"])
    with tune_routing.OUTPUT_PATH.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    assert len(rows) == 3
    assert rows[0]["target_fpr"] == ""
    assert [row["target_fpr"] for row in rows[1:]] == ["0.001", "0.0005"]
    assert all(row["existing_queries"] == "37" for row in rows)
    assert all(row["absent_probes"] == "55" for row in rows)
    assert all(row["seed"] == "7" for row in rows)
    assert list(tune_routing.OUTPUT_PATH.parent.iterdir()) == [tune_routing.OUTPUT_PATH]
    assert "Wrote" in capsys.readouterr().out


def test_cli_defaults(dataset, monkeypatch):
    calls = []

    def fake_experiment(queries, probes, seed):
        calls.append((queries, probes, seed))
        return [{"configuration": "test"}]

    monkeypatch.setattr(tune_routing, "run_experiment", fake_experiment)
    tune_routing.main([])
    assert calls == [(10_000, 10_000, 42)]


@pytest.mark.parametrize("argument", ["--queries", "--absent-probes"])
def test_cli_rejects_nonpositive_count(argument):
    with pytest.raises(SystemExit) as error:
        tune_routing.main([argument, "0"])
    assert error.value.code == 2


def test_module_has_no_import_time_experiment(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Import must not read databases or build filters")

    monkeypatch.setattr(sqlite3, "connect", unexpected)
    monkeypatch.setattr(Bloom, "add", unexpected)
    namespace = runpy.run_path(tune_routing.__file__, run_name="import_only")
    assert callable(namespace["main"])
