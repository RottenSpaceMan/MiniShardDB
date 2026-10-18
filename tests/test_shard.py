import sqlite3

import pytest

import config
from shard import Shard


@pytest.fixture
def shard(tmp_path):
    with Shard(str(tmp_path / "shard.db")) as current:
        current.create()
        current.insert_customers([(1, "Customer", "customer@example.com")])
        yield current


def test_inserting_orders_invalidates_persisted_filter(shard):
    shard.insert_orders([(1, 1, 10.0, "2026-01-01", "pending")])
    shard.build_bloom()
    assert config.bloom_path(shard.path).exists()

    shard.insert_orders([(2, 1, 20.0, "2026-01-02", "pending")])

    assert shard.bloom is None
    assert shard.bloom_has(2)
    assert not config.bloom_path(shard.path).exists()
    with Shard(shard.path) as reopened:
        assert reopened.bloom_has(2)


@pytest.mark.parametrize("sql", [
    "INSERT INTO orders VALUES (2, 1, 20, '2026-01-02', 'pending')",
    "UPDATE orders SET order_id = 2",
    "DELETE FROM orders",
    "WITH ids AS (SELECT 2) INSERT INTO orders SELECT * FROM ids",
])
def test_execute_rejects_writes_that_could_stale_filter(shard, sql):
    shard.insert_orders([(1, 1, 10.0, "2026-01-01", "pending")])
    shard.build_bloom()
    with pytest.raises(ValueError, match="only supports SELECT"):
        shard.execute(sql, ())
    assert shard.execute("SELECT order_id FROM orders", ()) == [(1,)]
    assert config.bloom_path(shard.path).exists()


def test_failed_batch_is_rolled_back(shard):
    with pytest.raises(sqlite3.IntegrityError):
        shard.insert_orders([
            (1, 1, 10.0, "2026-01-01", "pending"),
            (1, 1, 20.0, "2026-01-02", "pending"),
        ])
    assert shard.execute("SELECT COUNT(*) FROM orders", ()) == [(0,)]


def test_bloom_only_accepts_routing_column(shard):
    with pytest.raises(ValueError, match="Only order_id"):
        shard.build_bloom("cust_id")
    with pytest.raises(ValueError, match="Only order_id"):
        shard.build_bloom("order_id); DROP TABLE orders; --")


def test_rebuild_does_not_load_existing_filter(tmp_path):
    path = tmp_path / "shard.db"
    config.bloom_path(path).write_bytes(b"invalid pickle")
    with Shard(str(path), rebuild_bloom=True) as current:
        assert current.bloom is None


def test_context_manager_closes_connection(tmp_path):
    with Shard(str(tmp_path / "shard.db")) as current:
        current.create()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        current.execute("SELECT 1", ())
