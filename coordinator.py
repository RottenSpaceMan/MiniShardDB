from config import shard_of

class Coordinator:
    """The brain: routing + merge. Holds a list of Shard objects."""
    def __init__(self, shards):
        self.shards = shards

    def route(self, parsed) -> list:
        """parsed = (where_col, where_val). Logic:
        col == 'cust_id'  -> self.shards[where_val % len(self.shards)]   # partition map!
        col == 'order_id' -> [s for s in self.shards if s.bloom_has(where_val)]  # bloom routing!
        no WHERE          -> all shards"""
        return []

    def run(self, sql, params, parsed):
        """targets = self.route(parsed)
        results = [s.execute(sql, params) for s in targets]
        return self.merge(results)"""

    def merge(self, results):
        """Each shard returns rows of (group_key, partial_sum). SUM and COUNT
        both just ADD across shards -- that's the whole trick."""
        totals = {}
        for shard_rows in results:
            for key, val in shard_rows:
                totals[key] = totals.get(key, 0) + val
        return totals