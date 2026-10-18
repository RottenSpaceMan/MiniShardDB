"""_summary_
A class that represents a shard of a database, consisting of an SQLite file and its associated Bloom filter.
Returns:
    _type_: _description_
"""
import sqlite3
from collections.abc import Sequence

from bloom import Bloom
import config
import os
import pickle

class Shard:
    """One SQLite file + its Bloom filter"""
    def __init__(self, path: str, rebuild_bloom: bool = False) -> None:
        self.con: sqlite3.Connection = sqlite3.connect(path)
        self.cur: sqlite3.Cursor = self.con.cursor()
        self.path: str = path
        self.bloom: Bloom | None = None
        
        bloom_path = config.bloom_path(path)
        if bloom_path.exists():
            with bloom_path.open("rb") as f: self.bloom = pickle.load(f)
            if rebuild_bloom:
                print(f"Rebuilding Bloom filter for {path}")
                self.bloom = None  # Rebuild the Bloom filter

    def create(self) -> None:
        """CREATE TABLE orders(...) and customers(...) if not exists. Add:
        con.execute('CREATE INDEX ... ON orders(order_id)')  -- your M1 B+ tree, in real life."""
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

    def insert_orders(self, rows: Sequence[tuple[object, ...]]) -> None:
        """Insert multiple orders into the shard.(tuples of (order_id, cust_id, amount, created_at, status))"""
        self.con.executemany('INSERT INTO orders VALUES (?,?,?,?,?)', rows)
        self.con.commit()
    
    def insert_customers(self, rows: Sequence[tuple[object, ...]]) -> None:
        """Insert multiple customers into the shard.(tuples of (cust_id, name, email))"""
        self.con.executemany('INSERT INTO customers VALUES (?,?,?)', rows)
        self.con.commit()

    def build_bloom(
        self,
        column: str = "order_id",
        p: float = config.BLOOM_FALSE_POSITIVE_RATE,
    ) -> None:
        """Generate a Bloom filter for the specified column in the orders table and save it to a file."""
        n = self.con.execute(f"SELECT COUNT({column}) FROM orders").fetchone()[0]
        if n == 0:
            self.bloom = None
            bloom_path = config.bloom_path(self.path)
            if bloom_path.exists():
                bloom_path.unlink()
            return
        b = Bloom(n, p)
        for (v,) in self.con.execute(f"SELECT {column} FROM orders"):
            b.add(v)
        self.bloom = b
        with config.bloom_path(self.path).open("wb") as f: pickle.dump(b, f)

    def execute(self, sql: str, params: Sequence[object]) -> list[tuple[object, ...]]:
        """Execute a SQL query with parameters and return the results."""
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
    def __del__(self) -> None:
        """Close the database connection when the Shard object is deleted."""
        self.con.close()