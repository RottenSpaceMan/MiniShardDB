import sys
from pathlib import Path
from coordinator import Coordinator
from security import AuthGate
from shard import Shard
from parser import parse_where
import config

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python cli.py \"SQL query\"")
    query, params = sys.argv[1], []
    gate = AuthGate(config.META_DB)
    user = gate.login()
    if not gate.allow(user, query):
        sys.exit("DENIED (see audit log)")

    shards = [
        Shard(str(Path(config.SHARD_DIR) / f"shard{i}.db"))
        for i in range(config.NUM_SHARDS)
    ]
    for s in shards: s.build_bloom()
    coord = Coordinator(shards)
    for row in coord.run(query, params, parse_where(query)):
        print(row)