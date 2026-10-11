NUM_SHARDS = 8
SHARD_DIR = "data/shards/"
META_DB = "data/meta.db"
BASE_DB = "data/base.db"

def shard_of(cust_id: int) -> int:
    """
    Determine the shard for a given customer ID.
    """
    return cust_id % NUM_SHARDS