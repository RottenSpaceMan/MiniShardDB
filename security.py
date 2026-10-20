"""Demo-user authorization and persistent query auditing."""

import re
import sqlite3
from contextlib import closing
from pathlib import Path

import config
import parser
from models import AuditRow, PathLike, User


class AuthGate:
    """Look up demo users and permissions, and record query decisions."""

    def __init__(self, meta_path: PathLike) -> None:
        """Initialize demo metadata only when its database does not exist."""
        self.meta_path = meta_path
        metadata_path = Path(meta_path)
        if not metadata_path.exists():
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with closing(sqlite3.connect(meta_path)) as conn, conn:
                conn.execute("""
                    CREATE TABLE users (
                        name TEXT PRIMARY KEY,
                        role TEXT NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE TABLE role_perms (
                        role TEXT NOT NULL,
                        op TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        PRIMARY KEY (role, op, table_name)
                    )
                """)
                conn.executemany("INSERT INTO users VALUES (?, ?)", [
                    ("amal", "admin"),
                    ("bob", "analyst"),
                    ("carol", "auditor"),
                ])
                conn.executemany("INSERT INTO role_perms VALUES (?, ?, ?)", [
                    ("admin", "*", "*"),
                    ("analyst", "SELECT", "orders"),
                    ("analyst", "SELECT", "customers"),
                    ("auditor", "SELECT", "*"),
                ])

    def login(self) -> User:
        """Select a demo user by name; this is not password authentication."""
        while True:
            input_name = input("Username: ")
            with closing(sqlite3.connect(self.meta_path)) as conn:
                result = conn.execute(
                    "SELECT role FROM users WHERE name = ?", (input_name,)
                ).fetchone()
            if result is None:
                print("Unknown username, try again?[y/n]: ")
                choice = input().lower()
                if choice == "n":
                    raise ValueError("Login failed")
            else:
                role = result[0]
                return User(input_name, role)

    def allow(self, user: User, sql: str) -> bool:
        """Check statement safety and the user's role/table permissions."""
        if not parser.is_single_statement(sql):
            return False
        table = parser.parse_table(sql)
        if table is None:
            return False
        op = sql.strip().split(maxsplit=1)[0].upper()
        if op not in {"SELECT", "INSERT", "UPDATE", "DELETE"}:
            return False

        if op == "SELECT":
            try:
                parser.parse_supported_query(sql)
            except ValueError:
                # Preserve the RBAC API's wildcard-table behavior for simple
                # reads while the CLI only executes parse_supported_query().
                if not re.match(
                    r"^SELECT\s+[A-Za-z0-9_.*,\s()]+\s+FROM\s+[A-Za-z_][A-Za-z0-9_]*\s*$",
                    " ".join(sql.strip().split()),
                    re.IGNORECASE,
                ):
                    return False

        with closing(sqlite3.connect(self.meta_path)) as conn:
            row = conn.execute(
                """
                SELECT 1 FROM role_perms
                WHERE role = ?
                  AND (op = ? OR op = '*')
                  AND (table_name = ? OR table_name = '*')
                LIMIT 1
                """,
                (user.role, op, table),
            ).fetchone()
            return row is not None

    def audit(
        self,
        user: User,
        sql: str,
        allowed: bool,
        shards_touched: int,
    ) -> None:
        """Persist an authorization decision and its selected-shard count."""
        Path(config.AUDIT_DB).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(config.AUDIT_DB)) as conn, conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit (
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user TEXT,
                    role TEXT,
                    sql TEXT,
                    allowed BOOLEAN,
                    shards INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO audit (user, role, sql, allowed, shards)
                VALUES (?, ?, ?, ?, ?)
            """, (user.name, user.role, sql, allowed, shards_touched))

    def read_audit(self, user: User) -> list[AuditRow]:
        """Return audit records for administrators and auditors."""
        if user.role not in {"admin", "auditor"}:
            raise PermissionError("Only administrators and auditors may read the audit log")

        audit_path = Path(config.AUDIT_DB)
        if not audit_path.exists():
            return []

        with closing(sqlite3.connect(audit_path)) as conn:
            table = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'audit'
                """
            ).fetchone()
            if table is None:
                return []
            return conn.execute(
                """
                SELECT ts, user, role, sql, allowed, shards
                FROM audit
                ORDER BY rowid
                """
            ).fetchall()