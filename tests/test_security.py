import sqlite3

import pytest

import config
from security import AuthGate, User


@pytest.fixture
def auth_gate(tmp_path, monkeypatch):
    meta_path = tmp_path / "meta.db"
    audit_path = tmp_path / "audit.db"
    monkeypatch.setattr(config, "AUDIT_DB", str(audit_path))
    return AuthGate(str(meta_path))


def test_analyst_can_select_orders_and_customers(auth_gate):
    user = User("bob", "analyst")

    assert auth_gate.allow(user, "SELECT * FROM orders")
    assert auth_gate.allow(user, "SELECT * FROM customers")


def test_analyst_cannot_modify_data(auth_gate):
    user = User("bob", "analyst")

    assert not auth_gate.allow(user, "INSERT INTO orders VALUES (1, 2, 10, '2026', 'new')")
    assert not auth_gate.allow(user, "UPDATE customers SET name = 'changed'")


def test_auditor_can_select_any_table_but_not_write(auth_gate):
    user = User("carol", "auditor")

    assert auth_gate.allow(user, "SELECT * FROM orders")
    assert auth_gate.allow(user, "SELECT * FROM unknown_table")
    assert not auth_gate.allow(user, "DELETE FROM orders WHERE order_id = 1")


def test_admin_has_wildcard_permission(auth_gate):
    user = User("amal", "admin")

    assert auth_gate.allow(user, "DELETE FROM orders WHERE order_id = 1")
    assert auth_gate.allow(user, "UPDATE customers SET name = 'changed'")


@pytest.mark.parametrize("sql", ["", "SELECT 1", "DROP TABLE orders"])
def test_malformed_or_unsupported_sql_is_denied(auth_gate, sql):
    assert not auth_gate.allow(User("amal", "admin"), sql)


def test_login_returns_user_from_metadata(auth_gate, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _prompt="": "bob")

    user = auth_gate.login()

    assert user.name == "bob"
    assert user.role == "analyst"


def test_login_rejects_unknown_user(auth_gate, monkeypatch):
    responses = iter(["unknown", "n"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(responses))

    with pytest.raises(ValueError, match="Login failed"):
        auth_gate.login()


def test_audit_persists_allowed_and_denied_decisions(auth_gate, tmp_path):
    user = User("bob", "analyst")
    allowed_sql = "SELECT * FROM orders"
    denied_sql = "DELETE FROM orders WHERE order_id = 1"

    auth_gate.audit(user, allowed_sql, True, 1)
    auth_gate.audit(user, denied_sql, False, 0)

    with sqlite3.connect(str(tmp_path / "audit.db")) as connection:
        rows = connection.execute(
            "SELECT user, role, sql, allowed, shards FROM audit ORDER BY rowid"
        ).fetchall()

    assert rows == [
        ("bob", "analyst", allowed_sql, 1, 1),
        ("bob", "analyst", denied_sql, 0, 0),
    ]

def test_safe_select_is_allowed(auth_gate):
    user = User("bob", "analyst")
    sql = "SELECT * FROM orders"

    assert auth_gate.allow(user, sql) is True
    auth_gate.audit(user, sql, True, 8)

    with sqlite3.connect(str(config.AUDIT_DB)) as conn:
        result = conn.execute(
            "SELECT user, role, sql, allowed, shards FROM audit"
        ).fetchone()

    assert result == ("bob", "analyst", sql, 1, 8)

def test_injection_payload_is_denied_and_audited(auth_gate):
    user = User("bob", "analyst")
    sql = "SELECT * FROM orders; DROP TABLE orders;"

    assert auth_gate.allow(user, sql) is False
    auth_gate.audit(user, sql, False, 0)

    with sqlite3.connect(str(config.AUDIT_DB)) as conn:
        result = conn.execute(
            "SELECT user, role, sql, allowed, shards FROM audit"
        ).fetchone()

    assert result == ("bob", "analyst", sql, 0, 0)