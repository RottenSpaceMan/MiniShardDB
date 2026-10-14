import sys
from pathlib import Path
from coordinator import Coordinator
from security import AuthGate, User
from shard import Shard
from parser import parse_where
import config

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python cli.py \"SQL query\"")
    query: str = sys.argv[1]
    params: list[object] = []
    gate: AuthGate = AuthGate(config.META_DB)
    user: User = gate.login()
    if not gate.allow(user, query):
        sys.exit("DENIED (see audit log)")

    shards: list[Shard] = [
        Shard(str(Path(config.SHARD_DIR) / f"shard{i}.db"))
        for i in range(config.NUM_SHARDS)
    ]
    for s in shards: s.build_bloom()
    coord: Coordinator = Coordinator(shards)
    for row in coord.run(query, params, parse_where(query)):
        print(row)