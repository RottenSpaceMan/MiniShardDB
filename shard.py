import sqlite3
from bloom import Bloom
import pickle, os

class Shard:
    """One SQLite file + its Bloom filter. All DB contact happens here."""
    def __init__(self, path, rebuild_bloom=False):
        self.con = sqlite3.connect(path)
        self.cur = self.con.cursor()
        self.path = path
        self.bloom = None
        
        if os.path.exists(f"{path}.bloom"):
            with open(f"{path}.bloom", "rb") as f: self.bloom = pickle.load(f)
            if rebuild_bloom:
                print(f"Rebuilding Bloom filter for {path}")
                self.bloom = None  # Rebuild the Bloom filter

    def create(self):
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

    def insert_orders(self, rows):
        """Insert multiple orders into the shard.(tuples of (order_id, cust_id, amount, created_at, status))"""
        self.con.executemany('INSERT INTO orders VALUES (?,?,?,?,?)', rows)
        self.con.commit()
    
    def insert_customers(self, rows):
        """Insert multiple customers into the shard.(tuples of (cust_id, name, email))"""
        self.con.executemany('INSERT INTO customers VALUES (?,?,?)', rows)
        self.con.commit()

    def build_bloom(self, column="order_id", p=0.01):
        """Generate a Bloom filter for the specified column in the orders table and save it to a file."""
        n = self.con.execute(f"SELECT COUNT({column}) FROM orders").fetchone()[0]
        if n == 0:
            self.bloom = None
            bloom_path = f"{self.path}.bloom"
            if os.path.exists(bloom_path):
                os.remove(bloom_path)
            return
        b = Bloom(n, p)
        for (v,) in self.con.execute(f"SELECT {column} FROM orders"):
            b.add(v)
        self.bloom = b
        with open(f"{self.path}.bloom", "wb") as f: pickle.dump(b, f)

    def execute(self, sql, params) -> list:
        """Execute a SQL query with parameters and return the results."""
        self.cur.execute(sql, params)
        return self.cur.fetchall()

    def bloom_has(self, key) -> bool:
        """Check if the key is in the Bloom filter. If the Bloom filter is None, return True (must scan)."""
        if self.bloom is None:
            return True
        return key in self.bloom
    
    def close(self):
        """Close the database connection."""
        self.con.close()
    def __del__(self):
        """Close the database connection when the Shard object is deleted."""
        self.con.close()