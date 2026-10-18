"""Canonical domain models and shared type contracts.

Runtime-facing database APIs still use tuples because that is SQLite's native
row representation. The aliases below give those tuples one shared name,
while dataclasses describe records that cross application boundaries.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeAlias

SqlValue: TypeAlias = object
SqlParams: TypeAlias = Sequence[SqlValue]
SqlRow: TypeAlias = tuple[SqlValue, ...]
SqlRows: TypeAlias = list[SqlRow]
QueryResults: TypeAlias = Sequence[Sequence[SqlRow]]
NumericValue: TypeAlias = int | float
GroupedResults: TypeAlias = dict[SqlValue, NumericValue]
MergedResults: TypeAlias = SqlRows | GroupedResults

ParsedWhere: TypeAlias = tuple[str, int] | None
CustomerRow: TypeAlias = tuple[int, str, str]
OrderRow: TypeAlias = tuple[int, int, float, str, str]
CustomerSpec: TypeAlias = tuple[int, int, int]
AuditRow: TypeAlias = SqlRow


@dataclass(frozen=True)
class ParsedQuery:
    """A validated read query and its separately bound values."""

    sql: str
    params: tuple[SqlValue, ...]
    table: str
    where: ParsedWhere


@dataclass(frozen=True)
class User:
    """A logged-in user and their authorization role."""

    name: str
    role: str


@dataclass(frozen=True)
class CustomerRecord:
    """A customer record generated before it is written to SQLite."""

    cust_id: int
    name: str
    email: str

    def as_row(self) -> CustomerRow:
        return self.cust_id, self.name, self.email


@dataclass(frozen=True)
class OrderRecord:
    """An order record generated before it is written to SQLite."""

    order_id: int
    cust_id: int
    amount: float
    created_at: str
    status: str

    def as_row(self) -> OrderRow:
        return (
            self.order_id,
            self.cust_id,
            self.amount,
            self.created_at,
            self.status,
        )


@dataclass(frozen=True)
class GeneratedData:
    """The generated customer and order records for one worker batch."""

    customers: tuple[CustomerRecord, ...]
    orders: tuple[OrderRecord, ...]


@dataclass(frozen=True)
class GenerationOptions:
    """Validated command-line options for the data generator."""

    num_orders: int
    customer_max_orders: int
    seed: int
    rebuild_bloom: bool
    fresh: bool
    batch_size: int
    workers: int | None


@dataclass(frozen=True)
class ShardJob:
    """Serializable work item executed by a shard worker process."""

    database_path: str
    sql: str
    params: tuple[SqlValue, ...]


class ShardLike(Protocol):
    """The shard behavior required by routing and query execution."""

    path: str

    def bloom_has(self, key: SqlValue) -> bool:
        ...

    def execute(self, sql: str, params: SqlParams) -> SqlRows:
        ...


class ShardTarget(Protocol):
    """The shard behavior required to create a worker job."""

    path: str


PathLike: TypeAlias = str | Path
