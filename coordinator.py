from collections.abc import Sequence

import executor
from config import shard_of
from models import (
    GroupedResults,
    MergedResults,
    ParsedWhere,
    QueryResults,
    ShardLike,
    SqlParams,
)
from parser import has_group_by


class Coordinator:
    """Route queries, choose an execution path, and merge shard results."""
    def __init__(self, shards: Sequence[ShardLike]) -> None:
        self.shards = shards

    def route(self, parsed: ParsedWhere) -> list[ShardLike]:
        """Select candidate shards from a parsed WHERE clause."""
        if parsed is None:
            return list(self.shards)
        col, val = parsed
        if col == "cust_id":
            return [self.shards[shard_of(val)]]
        if col == "order_id":
            return [s for s in self.shards if s.bloom_has(val)]
        # A predicate on another column cannot be mapped to one shard safely.
        return list(self.shards)

    def run(
        self,
        sql: str,
        params: SqlParams,
        parsed: ParsedWhere,
        workers: int | None = None,
    ) -> MergedResults:
        """Run a query against the appropriate shards and merge the results."""
        targets: list[ShardLike] = self.route(parsed)
        grouped: bool = has_group_by(sql)
        results: QueryResults
        if parsed is not None and parsed[0] == "order_id" and not grouped:
            # Indexed lookups are cheaper than process dispatch; reuse shard connections.
            results = [shard.execute(sql, params) for shard in targets]
        else:
            results = executor.parallel(targets, sql, params, workers=workers)
        return self.merge(results, grouped=grouped)

    def merge(self, results: QueryResults, grouped: bool = False) -> MergedResults:
        """Flatten normal rows or sum values for grouped (key, value) rows."""
        if not grouped:
            return [row for shard_rows in results for row in shard_rows]
        totals: GroupedResults = {}
        for shard_rows in results:
            for row in shard_rows:
                if len(row) != 2:
                    raise TypeError("Grouped queries must return (key, numeric_value) rows")
                key, value = row
                if not isinstance(value, (int, float)):
                    raise TypeError("Grouped query values must be numeric")
                totals[key] = totals.get(key, 0) + value
        return totals