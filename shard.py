"""A SQLite shard with a persisted order-ID Bloom filter."""
import sqlite3
from collections.abc import Sequence

from bloom import Bloom
import config
import pickle
from models import SqlParams, SqlRow, SqlRows

class Shard:
    """One SQLite file + its Bloom filter"""
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
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                cust_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                cust_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (cust_id) REFERENCES customers(cust_id)
            )
        """)
        self.con.commit()

    def insert_orders(self, rows: Sequence[SqlRow]) -> None:
        """Insert multiple orders into the shard.(tuples of (order_id, cust_id, amount, created_at, status))"""
        # A stale filter could hide newly committed orders from routing.
        self.bloom = None
        config.bloom_path(self.path).unlink(missing_ok=True)
        with self.con:
            self.con.executemany('INSERT INTO orders VALUES (?,?,?,?,?)', rows)
    
    def insert_customers(self, rows: Sequence[SqlRow]) -> None:
        """Insert multiple customers into the shard.(tuples of (cust_id, name, email))"""
        with self.con:
            self.con.executemany('INSERT INTO customers VALUES (?,?,?)', rows)

    def build_bloom(
        self,
        column: str = "order_id",
        p: float = config.BLOOM_FALSE_POSITIVE_RATE,
    ) -> None:
        """Build the order-ID filter used by query routing."""
        if column != "order_id":
            raise ValueError("Only order_id Bloom filters are supported by routing")
        n = self.con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        if n == 0:
            self.bloom = None
            bloom_path = config.bloom_path(self.path)
            if bloom_path.exists():
                bloom_path.unlink()
            return
        b = Bloom(n, p)
        for (v,) in self.con.execute("SELECT order_id FROM orders"):
            b.add(v)
        self.bloom = b
        with config.bloom_path(self.path).open("wb") as bloom_file:
            pickle.dump(b, bloom_file)

    def execute(self, sql: str, params: SqlParams) -> SqlRows:
        """Execute a SELECT; use insertion APIs to keep Bloom state consistent."""
        statement = sql.split(maxsplit=1)
        if not statement or statement[0].upper() != "SELECT":
            raise ValueError("Shard.execute only supports SELECT queries")
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def bloom_has(self, key: object) -> bool:
        """Check if the key is in the Bloom filter. If the Bloom filter is None, return True (must scan)."""
        if self.bloom is None:
            return True
        return key in self.bloom

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