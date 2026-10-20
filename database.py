"""Shared SQLite schema and connection setup.

Connection helpers return caller-owned connections. Use ``contextlib.closing``
when a read-only connection should close at the end of a block; SQLite's own
connection context manager handles transactions, not connection lifetime.
"""

import sqlite3
from pathlib import Path

from models import PathLike


def readonly_uri(path: PathLike) -> str:
    """Encode a local database path as a read-only SQLite URI."""
    return Path(path).resolve().as_uri() + "?mode=ro"


def connect_read_only(path: PathLike) -> sqlite3.Connection:
    """Open an existing database without allowing writes or file creation."""
    return sqlite3.connect(readonly_uri(path), uri=True)


def create_schema(connection: sqlite3.Connection) -> None:
    """Create the shared customer/order schema without committing caller work."""
    connection.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            cust_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            cust_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (cust_id) REFERENCES customers(cust_id)
        )
    """)
