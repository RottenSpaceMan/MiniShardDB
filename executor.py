import sqlite3
from collections.abc import Sequence
from multiprocessing import Pool
from typing import Protocol


SqlRow = tuple[object, ...]
ShardJob = tuple[str, str, Sequence[object]]


class ShardTarget(Protocol):
    path: str


def run_on_shard(args: ShardJob) -> list[SqlRow]:
    db_path, sql, params = args

    with sqlite3.connect(db_path) as con:
        return con.execute(sql, params).fetchall()


def parallel(
    targets: Sequence[ShardTarget],
    sql: str,
    params: Sequence[object] = (),
    workers: int | None = None,
) -> list[list[SqlRow]]:
    jobs = [(s.path, sql, params) for s in targets]
    with Pool(workers) as pool:
        return pool.map(run_on_shard, jobs)