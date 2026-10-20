"""Shared schema and read-only connection contracts."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from database import connect_read_only, create_schema, readonly_uri


@pytest.fixture
def database_path(tmp_path):
    path = tmp_path / "orders # café & data.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        create_schema(connection)
        connection.execute("INSERT INTO customers VALUES (1, 'Customer', 'customer@example.com')")
        connection.execute("INSERT INTO orders VALUES (42, 1, 10, '2026-01-01', 'pending')")
    return path


@pytest.mark.parametrize("use_string", [False, True])
def test_readonly_uri_encodes_paths(database_path, use_string):
    path = str(database_path) if use_string else database_path
    uri = readonly_uri(path)
    assert uri.startswith("file:")
    assert "%23" in uri
    assert "%20" in uri
    assert uri.endswith("?mode=ro")
    with closing(connect_read_only(path)) as connection:
        assert connection.execute("SELECT order_id FROM orders").fetchall() == [(42,)]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("DELETE FROM orders")


def test_readonly_connection_resolves_relative_paths(database_path, monkeypatch):
    monkeypatch.chdir(database_path.parent)
    with closing(connect_read_only(Path(database_path.name))) as connection:
        assert connection.execute("SELECT COUNT(*) FROM orders").fetchone() == (1,)


def test_readonly_connection_does_not_create_missing_database(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        connect_read_only(path)
    assert not path.exists()


def test_schema_is_idempotent_and_keeps_primary_key_lookup(database_path):
    with closing(sqlite3.connect(database_path)) as connection:
        create_schema(connection)
        assert connection.execute("SELECT order_id FROM orders").fetchall() == [(42,)]
        columns = connection.execute("PRAGMA table_info(orders)").fetchall()
        assert [column[1] for column in columns] == [
            "order_id", "cust_id", "amount", "created_at", "status",
        ]
        assert columns[0][2] == "INTEGER"
        assert columns[0][5] == 1
        foreign_key = connection.execute("PRAGMA foreign_key_list(orders)").fetchone()
        assert foreign_key[2:5] == ("customers", "cust_id", "cust_id")
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM orders WHERE order_id = ?", (42,)
        ).fetchone()
        assert "USING INTEGER PRIMARY KEY" in plan[3]


def test_schema_creation_does_not_commit_caller_transaction():
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE sentinel(value INTEGER)")
        connection.execute("INSERT INTO sentinel VALUES (1)")
        create_schema(connection)
        assert connection.in_transaction
        connection.rollback()
        assert connection.execute("SELECT * FROM sentinel").fetchall() == []
