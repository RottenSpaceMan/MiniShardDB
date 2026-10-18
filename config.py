from pathlib import Path


NUM_SHARDS: int = 8
SHARD_DIR: Path = Path("data/shards")
META_DB: Path = Path("data/meta.db")
BASE_DB: Path = Path("data/base.db")
AUDIT_DB: Path = Path("data/audit.db")

SHARD_FILENAME_PREFIX: str = "shard"
BLOOM_FILENAME_SUFFIX: str = ".bloom"
BLOOM_FALSE_POSITIVE_RATE: float = 0.01

ORDER_AMOUNT_MIN: float = 10.0
ORDER_AMOUNT_MAX: float = 500.0
ORDER_STATUSES: tuple[str, ...] = ("pending", "shipped", "delivered", "canceled")

DEFAULT_MAX_WORKERS: int = 8


def shard_path(shard_id: int) -> Path:
    """Return the database path for one shard."""
    return SHARD_DIR / f"{SHARD_FILENAME_PREFIX}{shard_id}.db"


def bloom_path(shard_database: str | Path) -> Path:
    """Return the Bloom filter path associated with a shard database."""
    return Path(f"{shard_database}{BLOOM_FILENAME_SUFFIX}")


def shard_of(cust_id: int) -> int:
    """
    Determine the shard for a given customer ID.
    """
    return cust_id % NUM_SHARDS