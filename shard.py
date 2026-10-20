"""A SQLite shard with a persisted order-ID Bloom filter."""

import pickle
import sqlite3
from collections.abc import Sequence

import config
from bloom import Bloom
from database import create_schema
from models import SqlParams, SqlRow, SqlRows


class Shard:
    """Own one SQLite connection and its optional order-ID filter."""
    def __init__(self, path: str, rebuild_bloom: bool = False) -> None:
        self.path: str = path
        self.bloom: Bloom | None = None
        bloom_path = config.bloom_path(path)
        if not rebuild_bloom and bloom_path.exists():
            # Bloom files are trusted local artifacts, not an interchange format.
            with bloom_path.open("rb") as bloom_file:
                self.bloom = pickle.load(bloom_file)

        self.con: sqlite3.Connection = sqlite3.connect(path)
        self.cur: sqlite3.Cursor = self.con.cursor()

    def create(self) -> None:
        """Create the co-located customer and order tables if needed."""
        create_schema(self.con)
        self.con.commit()

    def _invalidate_bloom(self) -> None:
        self.bloom = None
        config.bloom_path(self.path).unlink(missing_ok=True)

    def insert_orders(self, rows: Sequence[SqlRow]) -> None:
        """Insert order rows and invalidate membership metadata before writing."""
        # A stale filter could hide newly committed orders from routing.
        self._invalidate_bloom()
        with self.con:
            self.con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", rows)

    def insert_customers(self, rows: Sequence[SqlRow]) -> None:
        """Insert customer rows in schema column order."""
        with self.con:
            self.con.executemany("INSERT INTO customers VALUES (?,?,?)", rows)

    def build_bloom(
        self,
        column: str = "order_id",
        p: float = config.BLOOM_FALSE_POSITIVE_RATE,
    ) -> None:
        """Build the order-ID filter used by query routing."""
        if column != "order_id":
            raise ValueError("Only order_id Bloom filters are supported by routing")
        order_count = self.con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        if order_count == 0:
            self._invalidate_bloom()
            return
        bloom = Bloom(order_count, p)
        for (order_id,) in self.con.execute("SELECT order_id FROM orders"):
            bloom.add(order_id)
        self.bloom = bloom
        with config.bloom_path(self.path).open("wb") as bloom_file:
            pickle.dump(bloom, bloom_file)

    def execute(self, sql: str, params: SqlParams) -> SqlRows:
        """Execute a SELECT; use insertion APIs to keep Bloom state consistent."""
        statement = sql.split(maxsplit=1)
        if not statement or statement[0].upper() != "SELECT":
            raise ValueError("Shard.execute only supports SELECT queries")
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def bloom_has(self, key: object) -> bool:
        """Keep a shard as a candidate when its filter is missing."""
        return self.bloom is None or key in self.bloom

    def close(self) -> None:
        """Close the database connection."""
        self.con.close()

    def __enter__(self) -> "Shard":
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        self.close()