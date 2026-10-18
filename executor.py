"""Execute SQL work against shard database files."""

import sqlite3
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from pathlib import Path

from models import ShardJob, ShardTarget, SqlParams, SqlRow, SqlRows


def make_jobs(
    targets: Sequence[ShardTarget],
    sql: str,
    params: SqlParams,
) -> list[ShardJob]:
    """Build serializable jobs for shard worker processes."""
    return [
        ShardJob(target.path, sql, tuple(params))
        for target in targets
    ]


def run_on_shard(args: ShardJob) -> list[SqlRow]:
    """Execute one SQL job using a short-lived SQLite connection."""
    database_uri = Path(args.database_path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(database_uri, uri=True)) as con:
        return con.execute(args.sql, args.params).fetchall()


def parallel(
    targets: Sequence[ShardTarget],
    sql: str,
    params: SqlParams = (),
    workers: int | None = None,
) -> list[SqlRows]:
    """Run shard jobs with a newly created process pool."""
    jobs = make_jobs(targets, sql, params)
    if not jobs:
        return []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(run_on_shard, jobs))


class PersistentPool:
    """Reuse one process pool across multiple shard queries."""

    def __init__(self, workers: int | None = None) -> None:
        """Create a process pool intended for repeated shard queries."""
        self._executor = ProcessPoolExecutor(max_workers=workers)

    def parallel(
        self,
        targets: Sequence[ShardTarget],
        sql: str,
        params: SqlParams = (),
    ) -> list[SqlRows]:
        """Run shard jobs on the persistent process pool."""
        jobs = make_jobs(targets, sql, params)
        return list(self._executor.map(run_on_shard, jobs))

    def close(self) -> None:
        """Shut down the worker pool and wait for its processes."""
        self._executor.shutdown()

    def __enter__(self) -> "PersistentPool":
        """Return this pool for use as a context manager."""
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        """Close the pool when leaving a context manager."""
        self.close()