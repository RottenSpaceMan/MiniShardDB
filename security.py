import sqlite3, getpass

class User:
    def __init__(self, name, role):
        self.name, self.role = name, role

class AuthGate:
    def __init__(self, meta_path):
        """One-time: create meta.db with tables
        users(name TEXT, role TEXT)  -- seed: alice/admin, bob/analyst, carol/auditor
        role_perms(role TEXT, op TEXT, table_name TEXT)
        role_perms rows: admin/*/*, analyst/SELECT/orders, analyst/SELECT/customers, auditor/SELECT/*"""

    def login(self) -> User:
        """input() username, look up role, return User (reject unknown names)"""

    def allow(self, user: User, sql: str) -> bool:
        """op = sql.strip().split()[0].upper()
        table = regex FROM/INTO/UPDATE ... (good enough: next word after FROM)
        return (user.role, op, table) matches a perm row -- then audit() the decision"""

    def audit(self, user, sql, allowed, shards_touched):
        """INSERT INTO audit(ts, user, role, sql, allowed, shards) -- auditor role reads this later"""