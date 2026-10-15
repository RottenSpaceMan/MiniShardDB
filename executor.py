import sqlite3
from collections.abc import Sequence
from typing import Protocol
from concurrent.futures import ProcessPoolExecutor

SqlRow = tuple[object, ...]
ShardJob = tuple[str, str, Sequence[object]]


class ShardTarget(Protocol):
    path: str


def make_jobs(
    targets: Sequence[ShardTarget],
    sql: str,
    params: Sequence[object],
) -> list[ShardJob]:
    return [(target.path, sql, params) for target in targets]


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
    jobs = make_jobs(targets, sql, params)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(run_on_shard, jobs))


class PersistentPool:
    """Reuse one process pool across multiple shard queries."""

    def __init__(self, workers: int | None = None) -> None:
        self._executor = ProcessPoolExecutor(max_workers=workers)

    def parallel(
        self,
        targets: Sequence[ShardTarget],
        sql: str,
        params: Sequence[object] = (),
    ) -> list[list[SqlRow]]:
        jobs = make_jobs(targets, sql, params)
        return list(self._executor.map(run_on_shard, jobs))

    def close(self) -> None:
        self._executor.shutdown()

    def __enter__(self) -> "PersistentPool":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()