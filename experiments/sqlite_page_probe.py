"""Measure SQLite page-cache hits/misses, not physical disk I/O.

Only stepping and fetching a statement are measured; prepare-time page accesses
are excluded. A fresh connection has a cold SQLite cache, not a cold OS cache.
Connections are intended for sequential use from a single thread.
"""

import ctypes
import ctypes.util
import sqlite3
import sys
from pathlib import Path



SQLITE_OK = 0
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_OPEN_READONLY = 1
SQLITE_DBSTATUS_CACHE_HIT = 7
SQLITE_DBSTATUS_CACHE_MISS = 8


def _load_sqlite() -> ctypes.CDLL:
    if sys.platform == "win32":
        # Use CPython's SQLite build rather than an unrelated DLL on PATH.
        path = Path(sys.base_prefix) / "DLLs" / "sqlite3.dll"
        if not path.is_file():
            raise OSError(f"CPython SQLite runtime not found: {path}")
        library = ctypes.CDLL(str(path))
    else:
        name = ctypes.util.find_library("sqlite3")
        if not name:
            raise OSError("SQLite shared library not found")
        library = ctypes.CDLL(name)

    pointer = ctypes.c_void_p
    integer = ctypes.c_int
    signatures = {
        "sqlite3_open_v2": (integer, [ctypes.c_char_p, ctypes.POINTER(pointer), integer, ctypes.c_char_p]),
        "sqlite3_close_v2": (integer, [pointer]),
        "sqlite3_errmsg": (ctypes.c_char_p, [pointer]),
        "sqlite3_libversion": (ctypes.c_char_p, []),
        "sqlite3_prepare_v2": (integer, [pointer, ctypes.c_char_p, integer, ctypes.POINTER(pointer), ctypes.POINTER(ctypes.c_char_p)]),
        "sqlite3_bind_parameter_count": (integer, [pointer]),
        "sqlite3_bind_int64": (integer, [pointer, integer, ctypes.c_int64]),
        "sqlite3_step": (integer, [pointer]),
        "sqlite3_finalize": (integer, [pointer]),
        "sqlite3_column_count": (integer, [pointer]),
        "sqlite3_column_type": (integer, [pointer, integer]),
        "sqlite3_column_int64": (ctypes.c_int64, [pointer, integer]),
        "sqlite3_column_double": (ctypes.c_double, [pointer, integer]),
        # Keep raw pointers so embedded NUL bytes are not truncated.
        "sqlite3_column_text": (pointer, [pointer, integer]),
        "sqlite3_column_blob": (pointer, [pointer, integer]),
        "sqlite3_column_bytes": (integer, [pointer, integer]),
        "sqlite3_db_status": (integer, [pointer, integer, ctypes.POINTER(integer), ctypes.POINTER(integer), integer]),
    }
    for name, (result, arguments) in signatures.items():
        function = getattr(library, name)
        function.restype = result
        function.argtypes = arguments
    return library


_SQLITE = _load_sqlite()


class PageProbe:
    """Read-only SQLite connection with per-execution page-cache counters.

    ``sqlite_version`` identifies the loaded C runtime for benchmark metadata.
    Use a context manager or call ``close()`` to release the connection.
    """

    sqlite_version: str = _SQLITE.sqlite3_libversion().decode("utf-8")

    def __init__(self, path: Path) -> None:
        self._db = ctypes.c_void_p()
        filename = str(path.resolve()).encode("utf-8")
        if b"\0" in filename:
            raise ValueError("database path contains a NUL byte")
        result = _SQLITE.sqlite3_open_v2(
            filename, ctypes.byref(self._db), SQLITE_OPEN_READONLY, None
        )
        if result != SQLITE_OK:
            error = self._error("open", result)
            self.close()
            raise error

    def _error(self, operation: str, result: int) -> sqlite3.OperationalError:
        message = _SQLITE.sqlite3_errmsg(self._db) if self._db else b"no database handle"
        return sqlite3.OperationalError(
            f"SQLite {operation} failed ({result}): {message.decode('utf-8', errors='replace')}"
        )

    def _check(self, result: int, operation: str) -> None:
        if result != SQLITE_OK:
            raise self._error(operation, result)

    def _require_open(self) -> None:
        if not self._db:
            raise sqlite3.ProgrammingError("Cannot operate on a closed PageProbe")

    def _cache_count(self, operation: int, *, reset: bool = False) -> int:
        current = ctypes.c_int()
        highwater = ctypes.c_int()
        self._check(
            _SQLITE.sqlite3_db_status(
                self._db, operation, ctypes.byref(current),
                ctypes.byref(highwater), int(reset),
            ),
            "db_status",
        )
        return current.value

    @staticmethod
    def _column(statement: ctypes.c_void_p, index: int) -> object:
        kind = _SQLITE.sqlite3_column_type(statement, index)
        if kind == 1:
            return _SQLITE.sqlite3_column_int64(statement, index)
        if kind == 2:
            return _SQLITE.sqlite3_column_double(statement, index)
        if kind == 5:
            return None
        if kind == 3:
            pointer = _SQLITE.sqlite3_column_text(statement, index)
        elif kind == 4:
            pointer = _SQLITE.sqlite3_column_blob(statement, index)
        else:
            raise sqlite3.DatabaseError(f"Unknown SQLite column type: {kind}")
        length = _SQLITE.sqlite3_column_bytes(statement, index)
        if not pointer and (kind == 3 or length):
            raise MemoryError("SQLite could not materialize column data")
        value = ctypes.string_at(pointer, length) if length else b""
        return value.decode("utf-8") if kind == 3 else value

    def execute(
        self, sql: str, params: tuple[int, ...]
    ) -> tuple[list[tuple[object, ...]], int, int]:
        """Return ``(rows, hits, misses)`` for one statement with int64 params.

        Counters are reset after prepare/bind, immediately before stepping.
        SQLite's OS cache and storage-device activity are not measured.
        """
        self._require_open()
        encoded = sql.encode("utf-8")
        if b"\0" in encoded:
            raise ValueError("SQL contains a NUL byte")
        statement = ctypes.c_void_p()
        tail = ctypes.c_char_p()
        try:
            self._check(
                _SQLITE.sqlite3_prepare_v2(
                    self._db, encoded, -1, ctypes.byref(statement), ctypes.byref(tail)
                ),
                "prepare",
            )
            if not statement:
                raise sqlite3.ProgrammingError("SQL must contain a statement")
            if tail.value and tail.value.strip():
                raise sqlite3.ProgrammingError("Only one SQL statement is supported")
            count = _SQLITE.sqlite3_bind_parameter_count(statement)
            if len(params) != count:
                raise sqlite3.ProgrammingError(
                    f"Expected {count} parameters, received {len(params)}"
                )
            for index, value in enumerate(params, start=1):
                if not isinstance(value, int):
                    raise TypeError("PageProbe parameters must be integers")
                if not -(1 << 63) <= value < (1 << 63):
                    raise OverflowError("Parameter is outside the signed int64 range")
                self._check(
                    _SQLITE.sqlite3_bind_int64(statement, index, value), "bind"
                )
            rows: list[tuple[object, ...]] = []
            columns = _SQLITE.sqlite3_column_count(statement)
            self._cache_count(SQLITE_DBSTATUS_CACHE_HIT, reset=True)
            self._cache_count(SQLITE_DBSTATUS_CACHE_MISS, reset=True)
            while True:
                result = _SQLITE.sqlite3_step(statement)
                if result == SQLITE_DONE:
                    break
                if result != SQLITE_ROW:
                    raise self._error("step", result)
                rows.append(tuple(self._column(statement, i) for i in range(columns)))
            hits = self._cache_count(SQLITE_DBSTATUS_CACHE_HIT)
            misses = self._cache_count(SQLITE_DBSTATUS_CACHE_MISS)
            return rows, hits, misses
        finally:
            if statement:
                result = _SQLITE.sqlite3_finalize(statement)
                # Finalize can repeat a step failure; preserve the original error.
                if sys.exc_info()[0] is None:
                    self._check(result, "finalize")

    def close(self) -> None:
        """Close the connection; repeated calls are harmless."""
        if self._db:
            self._check(_SQLITE.sqlite3_close_v2(self._db), "close")
            self._db = ctypes.c_void_p()

    def __enter__(self) -> "PageProbe":
        self._require_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def __del__(self) -> None:
        database = getattr(self, "_db", None)
        if database:
            _SQLITE.sqlite3_close_v2(database)
