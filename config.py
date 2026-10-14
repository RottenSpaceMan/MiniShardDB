NUM_SHARDS: int = 8
SHARD_DIR: str = "data/shards/"
META_DB: str = "data/meta.db"
BASE_DB: str = "data/base.db"
AUDIT_DB: str = "data/audit.db"
def shard_of(cust_id: int) -> int:
    """
    Determine the shard for a given customer ID.
    """
    return cust_id % NUM_SHARDS