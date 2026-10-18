import argparse
import sys
import config
from coordinator import Coordinator
from parser import parse_supported_query
from security import AuthGate, User
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
        rows = getattr(gate, "read_audit")(user)
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
    except ValueError as e:
        gate.audit(user, query, allowed=False, shards_touched=0)
        sys.exit(f"DENIED: {e}")

    if not gate.allow(user, query):
        gate.audit(user, query, allowed=False, shards_touched=0)
        sys.exit("DENIED (see audit log)")

    shards: list[Shard] = [
        Shard(str(config.shard_path(i)))
        for i in range(config.NUM_SHARDS)
    ]
    for s in shards:
        s.build_bloom()

    try:
        coord: Coordinator = Coordinator(shards)
        results = coord.run(parsed.sql, parsed.params, parsed.where)
        gate.audit(
            user,
            query,
            allowed=True,
            shards_touched=len(coord.route(parsed.where)),
        )
        for row in results:
            print(row)
    finally:
        for shard in shards:
            shard.close()

if __name__ == "__main__":
    main()