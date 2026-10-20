
from collections.abc import Sequence
from contextlib import ExitStack

import pytest

import config
import executor
from coordinator import Coordinator
from parser import has_group_by, parse_where
from shard import Shard

class FakeShard:
    path: str

    def __init__(
        self,
        name: str,
        bloom_match: bool = False,
        rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.name = name
        self.path = name
        self.bloom_match = bloom_match
        self.rows = rows or []

    def bloom_has(self, key: object) -> bool:
        return self.bloom_match

    def execute(
        self,
        sql: str,
        params: Sequence[object],
    ) -> list[tuple[object, ...]]:
        return self.rows


def test_cust_id_routes_to_one_shard():
    shards = [FakeShard(f"shard{i}") for i in range(8)]
    coord = Coordinator(shards)
    assert coord.route(("cust_id", 42)) == [shards[2]]


def test_no_where_routes_to_all_shards():
    shards = [FakeShard(f"shard{i}") for i in range(3)]
    coord = Coordinator(shards)
    assert coord.route(None) == shards


def test_parse_where_returns_none_without_predicate():
    assert parse_where("SELECT * FROM orders") is None


def test_parse_where_returns_column_and_value():
    assert parse_where("SELECT * FROM orders WHERE cust_id = 42") == ("cust_id", 42)


def test_has_group_by_detects_grouped_queries():
    assert has_group_by("SELECT status, SUM(amount) FROM orders GROUP BY status")
    assert not has_group_by("SELECT * FROM orders")


def test_order_id_routes_to_matching_bloom_shards():
    shards = [FakeShard("shard0"), FakeShard("shard1", bloom_match=True)]
    coord = Coordinator(shards)
    assert coord.route(("order_id", 123)) == [shards[1]]


def test_unknown_filter_routes_to_all_shards():
    shards = [FakeShard(f"shard{i}") for i in range(3)]
    coord = Coordinator(shards)
    assert coord.route(("status", 1)) == shards


def test_select_star_results_are_flattened_from_data_shards(tmp_path):
    with ExitStack() as resources:
        shards = []
        for index in range(config.NUM_SHARDS):
            shard = resources.enter_context(Shard(str(tmp_path / f"shard{index}.db")))
            shard.create()
            shard.insert_customers([(index, "Customer", "customer@example.com")])
            shard.insert_orders([(index, index, 10.0, "2026-01-01", "pending")])
            shards.append(shard)
        results = Coordinator(shards).run("SELECT * FROM orders", (), None, workers=2)

    assert isinstance(results, list)
    assert len(results) == config.NUM_SHARDS
    assert all(len(row) == 5 for row in results)


def test_order_lookup_reuses_shard_connections_and_queries_all_candidates(monkeypatch):
    calls = []

    class RecordingShard(FakeShard):
        def execute(self, sql, params):
            calls.append((self.name, sql, params))
            return self.rows

    def unexpected_pool(*args, **kwargs):
        pytest.fail("point lookups must not create a process pool")

    monkeypatch.setattr(executor, "parallel", unexpected_pool)
    shards = [
        RecordingShard("excluded"),
        RecordingShard("false_positive", bloom_match=True),
        RecordingShard("actual", bloom_match=True, rows=[(123, 10.0)]),
    ]
    sql = "SELECT order_id, amount FROM orders WHERE order_id = ?"
    coordinator = Coordinator(shards)
    for _ in range(2):
        assert coordinator.run(sql, (123,), ("order_id", 123), workers=8) == [(123, 10.0)]
    assert calls == [(name, sql, (123,)) for _ in range(2) for name in ("false_positive", "actual")]


def test_order_lookup_without_candidates_does_not_dispatch(monkeypatch):
    def unexpected_pool(*args, **kwargs):
        pytest.fail("absent lookups must not create a process pool")

    monkeypatch.setattr(executor, "parallel", unexpected_pool)
    assert Coordinator([FakeShard("excluded")]).run(
        "SELECT * FROM orders WHERE order_id = ?", (999,), ("order_id", 999)
    ) == []


@pytest.mark.parametrize("sql, parsed, grouped", [
    ("SELECT * FROM orders", None, False),
    ("SELECT * FROM orders WHERE cust_id = ?", ("cust_id", 42), False),
    ("SELECT * FROM orders WHERE status = ?", ("status", 1), False),
    ("SELECT status, SUM(amount) FROM orders GROUP BY status", None, True),
    ("SELECT status, SUM(amount) FROM orders WHERE order_id = ? GROUP BY status", ("order_id", 123), True),
])
def test_scans_customer_queries_and_grouped_queries_keep_parallel_execution(monkeypatch, sql, parsed, grouped):
    calls = []
    shards = [FakeShard(f"shard{i}", bloom_match=True) for i in range(config.NUM_SHARDS)]
    params = () if parsed is None else (parsed[1],)

    def parallel(targets, query, values, workers=None):
        calls.append((list(targets), query, values, workers))
        return [[("pending", 10)], [("pending", 20)]] if grouped else [[(1,)], [(2,)]]

    monkeypatch.setattr(executor, "parallel", parallel)
    coordinator = Coordinator(shards)
    assert coordinator.run(sql, params, parsed, workers=4) == ({"pending": 30} if grouped else [(1,), (2,)])
    assert calls == [(coordinator.route(parsed), sql, params, 4)]


def test_real_order_lookup_reuses_connection_and_missing_filters_fall_back_safely(tmp_path, monkeypatch):
    def unexpected_pool(*args, **kwargs):
        pytest.fail("point lookups must use existing shard connections")

    monkeypatch.setattr(executor, "parallel", unexpected_pool)
    with ExitStack() as resources:
        shards = []
        for index in range(2):
            shard = resources.enter_context(Shard(str(tmp_path / f"shard{index}.db")))
            shard.create()
            shard.insert_customers([(index, "Customer", "customer@example.com")])
            shard.insert_orders([(index, index, 10.0, "2026-01-01", "pending")])
            if index == 0:
                shard.build_bloom()
            shards.append(shard)
        coordinator = Coordinator(shards)
        connections = [shard.con for shard in shards]
        sql = "SELECT order_id, cust_id FROM orders WHERE order_id = ?"
        for _ in range(2):
            assert coordinator.run(sql, (1,), ("order_id", 1)) == [(1, 1)]
        assert coordinator.run(sql, (999,), ("order_id", 999)) == []
        assert [shard.con for shard in shards] == connections


def test_order_lookup_propagates_shard_errors():
    class FailingShard(FakeShard):
        def execute(self, sql, params):
            raise RuntimeError("shard unavailable")

    with pytest.raises(RuntimeError, match="shard unavailable"):
        Coordinator([FailingShard("broken", bloom_match=True)]).run(
            "SELECT * FROM orders WHERE order_id = ?", (123,), ("order_id", 123)
        )


def test_grouped_results_are_summed():
    results = [[("paid", 5), ("open", 2)], [("paid", 3)]]
    assert Coordinator([]).merge(results, grouped=True) == {"paid": 8, "open": 2}