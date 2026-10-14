from collections.abc import Sequence
from typing import Protocol

from config import shard_of
from parser import has_group_by

ParsedWhere = tuple[str, int] | None
SqlRow = tuple[object, ...]
QueryResults = Sequence[Sequence[SqlRow]]
MergedResults = list[SqlRow] | dict[object, int | float]


class _ShardLike(Protocol):
    path: str

    def bloom_has(self, key: object) -> bool:
        ...

    def execute(self, sql: str, params: Sequence[object]) -> list[SqlRow]:
        ...


class Coordinator:
    """Holds a list of Shard objects and routes queries to the right shard(s) based on WHERE clause."""
    def __init__(self, shards: Sequence[_ShardLike]) -> None:
        self.shards = shards

    def route(self, parsed: ParsedWhere) -> list[_ShardLike]:
        """Given a parsed WHERE clause, return the list of Shard objects that should be queried."""
        if parsed is None:
            return list(self.shards)
        col, val = parsed
        if col == 'cust_id':
            return [self.shards[shard_of(val)]]
        if col == 'order_id':
            return [s for s in self.shards if s.bloom_has(val)]
        # A predicate on another column cannot be mapped to one shard safely.
        return list(self.shards)

    def run(
        self,
        sql: str,
        params: Sequence[object],
        parsed: ParsedWhere,
    ) -> MergedResults:
        """Run a query against the appropriate shards and merge the results."""
        targets = self.route(parsed)
        results = [s.execute(sql, params) for s in targets]
        return self.merge(results, grouped=has_group_by(sql))

    def merge(self, results: QueryResults, grouped: bool = False) -> MergedResults:
        """Flatten normal rows or sum values for grouped (key, value) rows."""
        if not grouped:
            return [row for shard_rows in results for row in shard_rows]
        totals: dict[object, int | float] = {}
        for shard_rows in results:
            for row in shard_rows:
                if len(row) != 2:
                    raise TypeError("Grouped queries must return (key, numeric_value) rows")
                key, value = row
                if not isinstance(value, (int, float)):
                    raise TypeError("Grouped query values must be numeric")
                totals[key] = totals.get(key, 0) + value
        return totals