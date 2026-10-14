import sqlite3
from pathlib import Path
import config, parser

class User:
    def __init__(self, name: str, role: str) -> None:
        """Instance of a logged-in user with a name and role"""
        self.name, self.role = name, role

class AuthGate:
    def __init__(self, meta_path: str) -> None:
        """Instance of an authorization gate, which uses a meta database to look up users and permissions, current implementation generates the meta database if it doesn't exist"""
        self.meta_path = meta_path
        if not Path(meta_path).exists():
            conn = sqlite3.connect(meta_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE users (
                    name TEXT,
                    role TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE role_perms (
                    role TEXT,
                    op TEXT,
                    table_name TEXT
                )
            """)
            # Set up initial users and permissions
            cursor.executemany("INSERT INTO users VALUES (?, ?)", [
                ("amal", "admin"),
                ("bob", "analyst"),
                ("carol", "auditor"),
            ])
            cursor.executemany("INSERT INTO role_perms VALUES (?, ?, ?)", [
                ("admin", "*", "*"),
                ("analyst", "SELECT", "orders"),
                ("analyst", "SELECT", "customers"),
                ("auditor", "SELECT", "*"),
            ])
            conn.commit()
            conn.close()

    def login(self) -> User:
        """Prompt for username and password, check against the meta database, and return a User instance if successful. For simplicity, this implementation only checks the username and ignores the password."""
        while True:
            input_name = input("Username: ")
            conn = sqlite3.connect(self.meta_path)
            cursor = conn.cursor()
            cursor.execute("SELECT role FROM users WHERE name = ?", (input_name,))
            result = cursor.fetchone()
            if result is None:
                print("Unknown username, try again?[y/n]: ")
                choice = input().lower()
                if choice == 'n':
                    raise ValueError("Login failed")
            else:
                role = result[0]
                conn.close()
                return User(input_name, role)

    def allow(self, user: User, sql: str) -> bool:
        """Check if the given user is allowed to execute the given SQL query based on their role and the permissions defined in the meta database."""
        words = sql.strip().split()
        if not words:
            return False
        op = words[0].upper()
        table = parser.parse_table(sql)
        if table is None:
            return False

        conn = sqlite3.connect(self.meta_path)
        try:
            row = conn.execute(
                """
                SELECT 1 FROM role_perms WHERE role = ? AND (op = ? OR op = '*') AND (table_name = ? OR table_name = '*') LIMIT 1
                """, (user.role, op, table)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def audit(
        self,
        user: User,
        sql: str,
        allowed: bool,
        shards_touched: int,
    ) -> None:
        """Audit the execution of a SQL query by inserting a record into the audit database."""
        if not Path(config.AUDIT_DB).exists():
            conn = sqlite3.connect(config.AUDIT_DB)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE audit (
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user TEXT,
                    role TEXT,
                    sql TEXT,
                    allowed BOOLEAN,
                    shards INTEGER
                )
            """)
            conn.commit()
            conn.close()
        conn = sqlite3.connect(config.AUDIT_DB)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO audit (user, role, sql, allowed, shards) VALUES (?, ?, ?, ?, ?)
        """, (user.name, user.role, sql, allowed, shards_touched))
        conn.commit()
        conn.close()