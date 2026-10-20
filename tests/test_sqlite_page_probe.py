import sqlite3
from contextlib import closing

import pytest

from experiments.sqlite_page_probe import PageProbe


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "indexed café.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, lookup INTEGER, payload TEXT)"
        )
        connection.executemany(
            "INSERT INTO items VALUES (?, ?, ?)",
            ((i, i * 3, "payload-" + "x" * 200) for i in range(4096)),
        )
        connection.execute("CREATE INDEX items_lookup ON items(lookup)")
    return path


def test_cold_indexed_lookup_then_warm_hits(database):
    sql = "SELECT id, payload FROM items WHERE lookup = ?"
    expected = [(2048, "payload-" + "x" * 200)]
    # Each new connection has a fresh SQLite cache, even if the OS cache is warm.
    for _ in range(2):
        with PageProbe(database) as probe:
            rows, hits, misses = probe.execute(sql, (6144,))
            assert rows == expected
            assert hits >= 0
            assert misses > 0
            for _ in range(3):
                rows, hits, misses = probe.execute(sql, (6144,))
                assert rows == expected
                assert hits > 0
                assert misses == 0


def test_missing_result(database):
    with PageProbe(database) as probe:
        rows, hits, misses = probe.execute(
            "SELECT id FROM items WHERE lookup = ?", (-1,)
        )
        assert rows == []
        assert hits >= 0
        assert misses > 0
        rows, hits, misses = probe.execute(
            "SELECT id FROM items WHERE lookup = ?", (-1,)
        )
        assert rows == []
        assert hits > 0
        assert misses == 0


def test_counters_are_per_execution(database):
    with PageProbe(database) as probe:
        _, _, misses = probe.execute("SELECT payload FROM items WHERE lookup = ?", (6,))
        assert misses > 0
        assert probe.execute("SELECT 1", ()) == ([(1,)], 0, 0)
        assert probe.execute("SELECT 1", ()) == ([(1,)], 0, 0)


def test_counters_reset_after_prepare_immediately_before_step(database, monkeypatch):
    from experiments import sqlite_page_probe as module

    calls = []
    library = module._SQLITE
    prepare = library.sqlite3_prepare_v2
    step = library.sqlite3_step
    status = library.sqlite3_db_status

    def record_prepare(*args):
        calls.append("prepare")
        return prepare(*args)

    def record_step(*args):
        calls.append("step")
        return step(*args)

    def record_status(db, operation, current, highwater, reset):
        calls.append((operation, reset))
        return status(db, operation, current, highwater, reset)

    monkeypatch.setattr(library, "sqlite3_prepare_v2", record_prepare)
    monkeypatch.setattr(library, "sqlite3_step", record_step)
    monkeypatch.setattr(library, "sqlite3_db_status", record_status)
    with PageProbe(database) as probe:
        probe.execute("SELECT id FROM items WHERE lookup = ?", (6,))
    assert calls == ["prepare", (7, 1), (8, 1), "step", "step", (7, 0), (8, 0)]


def test_result_types_and_int64_parameters(database):
    with PageProbe(database) as probe:
        rows, _, _ = probe.execute(
            "SELECT ?, ?, 1.25, 'café' || char(0) || 'end', NULL, "
            "x'00FF0041', x'', ''",
            (-(1 << 63), (1 << 63) - 1),
        )
        assert rows == [
            (-(1 << 63), (1 << 63) - 1, 1.25, "café\0end", None, b"\x00\xff\x00A", b"", "")
        ]


def test_multiple_rows(database):
    with PageProbe(database) as probe:
        rows, _, _ = probe.execute(
            "SELECT id FROM items WHERE lookup < ? ORDER BY id", (9,)
        )
        assert rows == [(0,), (1,), (2,)]


@pytest.mark.parametrize("sql", [
    "INSERT INTO items VALUES (5000, 5000, 'new')",
    "UPDATE items SET payload = 'changed' WHERE id = 1",
    "DELETE FROM items WHERE id = 1",
    "CREATE TABLE forbidden (id INTEGER)",
])
def test_readonly_failures_leave_connection_usable(database, sql):
    with PageProbe(database) as probe:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            probe.execute(sql, ())
        assert probe.execute("SELECT count(*) FROM items", ())[0] == [(4096,)]
    # Also verifies no lingering C handle/statement keeps the file open on Windows.
    database.unlink()


def test_missing_database_is_not_created(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError, match="open"):
        PageProbe(path)
    assert not path.exists()
    # A failed open must release even the partially allocated SQLite handle.
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE created_later (id INTEGER)")
    path.unlink()


def test_context_manager_closes_connection(database):
    with PageProbe(database) as probe:
        assert probe.execute("SELECT 1", ())[0] == [(1,)]
    probe.close()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        probe.execute("SELECT 1", ())
    with pytest.raises(sqlite3.ProgrammingError, match="closed"), probe:
        pass
    database.unlink()


def test_context_manager_closes_on_exception(database):
    probe = PageProbe(database)
    with pytest.raises(RuntimeError, match="body failed"), probe:
        raise RuntimeError("body failed")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        probe.execute("SELECT 1", ())
    database.unlink()


@pytest.mark.parametrize(("sql", "params", "error"), [
    ("SELECT ?", (), sqlite3.ProgrammingError),
    ("SELECT 1", (1,), sqlite3.ProgrammingError),
    ("SELECT ?", (1 << 63,), OverflowError),
    ("SELECT ?", (-(1 << 63) - 1,), OverflowError),
    ("SELECT ?", (1.5,), TypeError),
    ("SELECT ?", ("1",), TypeError),
    ("SELECT 1\0; DELETE FROM items", (), ValueError),
    ("", (), sqlite3.ProgrammingError),
    ("SELECT 1; SELECT 2", (), sqlite3.ProgrammingError),
    ("SELECT FROM", (), sqlite3.OperationalError),
    ("SELECT CAST(x'FF' AS TEXT)", (), UnicodeDecodeError),
])
def test_errors_finalize_statements(database, sql, params, error):
    with PageProbe(database) as probe:
        with pytest.raises(error):
            probe.execute(sql, params)
        assert probe.execute("SELECT ?", (42,))[0] == [(42,)]
    database.unlink()


def test_version_matches_python_runtime(database):
    with PageProbe(database) as probe:
        assert probe.sqlite_version == sqlite3.sqlite_version
        assert probe.execute("SELECT sqlite_version()", ())[0] == [
            (probe.sqlite_version,)
        ]
