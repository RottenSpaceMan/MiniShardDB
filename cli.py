"""Run supported SQL queries or inspect the role-protected audit log."""

import argparse
import sys
from contextlib import ExitStack

import config
from coordinator import Coordinator
from models import User
from parser import parse_supported_query
from security import AuthGate
from shard import Shard


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a SQL query or view the audit log."
    )
    parser.add_argument("query", nargs="?", help="The SQL query to execute.")
    parser.add_argument(
        "--audit",
        action="store_true",
        help="View the audit log as an administrator or auditor.",
    )
    args = parser.parse_args()

    if args.audit == (args.query is not None):
        parser.error("provide either a SQL query or --audit")
    if args.audit:
        view_audit()
    else:
        run_query(args.query)


def view_audit() -> None:
    """Authenticate and print the audit log for an authorized role."""
    gate: AuthGate = AuthGate(config.META_DB)
    user: User = gate.login()
    try:
        rows = gate.read_audit(user)
    except PermissionError as error:
        sys.exit(f"DENIED: {error}")

    if not rows:
        print("Audit log is empty.")
        return

    print("timestamp | user | role | sql | allowed | shards")
    for row in rows:
        print(" | ".join(str(value) for value in row))


def run_query(query: str) -> None:
    gate: AuthGate = AuthGate(config.META_DB)
    user: User = gate.login()
    try:
        parsed = parse_supported_query(query)
    except ValueError as error:
        gate.audit(user, query, allowed=False, shards_touched=0)
        sys.exit(f"DENIED: {error}")

    if not gate.allow(user, query):
        gate.audit(user, query, allowed=False, shards_touched=0)
        sys.exit("DENIED (see audit log)")

    paths = [config.shard_path(i) for i in range(config.NUM_SHARDS)]
    if any(not path.is_file() for path in paths):
        sys.exit("Missing shard databases; run gen_data.py first.")

    with ExitStack() as resources:
        shards = [resources.enter_context(Shard(str(path))) for path in paths]
        coordinator = Coordinator(shards)
        results = coordinator.run(parsed.sql, parsed.params, parsed.where)
        gate.audit(
            user,
            query,
            allowed=True,
            shards_touched=len(coordinator.route(parsed.where)),
        )
        rows = results.items() if isinstance(results, dict) else results
        for row in rows:
            print(row)


if __name__ == "__main__":
    main()