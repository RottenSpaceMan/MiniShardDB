import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

import executor
from models import ShardJob


def test_worker_does_not_create_missing_database(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        executor.run_on_shard(ShardJob(str(path), "SELECT 1", ()))
    assert not path.exists()


def test_worker_executes_parameterized_reads(tmp_path):
    path = tmp_path / "shard.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE orders (order_id INTEGER)")
        connection.execute("INSERT INTO orders VALUES (42)")
    assert executor.run_on_shard(
        ShardJob(str(path), "SELECT order_id FROM orders WHERE order_id = ?", (42,))
    ) == [(42,)]
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        executor.run_on_shard(ShardJob(str(path), "DELETE FROM orders", ()))


def test_make_jobs_snapshots_parameters_for_all_targets():
    params = [42]
    targets = [SimpleNamespace(path="one.db"), SimpleNamespace(path="two.db")]
    jobs = executor.make_jobs(targets, "SELECT ?", params)
    params.append(99)
    assert jobs == [
        ShardJob("one.db", "SELECT ?", (42,)),
        ShardJob("two.db", "SELECT ?", (42,)),
    ]


def test_no_targets_does_not_create_pool(monkeypatch):
    def unexpected_pool(*args, **kwargs):
        pytest.fail("An empty query must not start worker processes")

    monkeypatch.setattr(executor, "ProcessPoolExecutor", unexpected_pool)
    assert executor.parallel([], "SELECT 1") == []
