from coordinator import Coordinator
from collections.abc import Sequence
from parser import has_group_by, parse_where

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


def test_select_star_results_are_flattened():
    shards = [
        FakeShard("shard0", rows=[(1, 10, 5.0, "2026-01-01", "paid")]),
        FakeShard("shard1", rows=[(2, 11, 7.0, "2026-01-02", "open")]),
    ]
    coord = Coordinator(shards)
    assert coord.run("SELECT * FROM orders", (), None) == [
        (1, 10, 5.0, "2026-01-01", "paid"),
        (2, 11, 7.0, "2026-01-02", "open"),
    ]


def test_grouped_results_are_summed():
    results = [[("paid", 5), ("open", 2)], [("paid", 3)]]
    assert Coordinator([]).merge(results, grouped=True) == {"paid": 8, "open": 2}