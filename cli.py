import sys
from pathlib import Path
from coordinator import Coordinator
from security import AuthGate, User
from shard import Shard
from parser import parse_supported_query
import config

def main()-> None:
    if len(sys.argv) != 2:
        sys.exit("Usage: python cli.py \"SQL query\"")
    run_query(sys.argv[1])

def run_query(query: str)-> None:
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
        Shard(str(Path(config.SHARD_DIR) / f"shard{i}.db"))
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