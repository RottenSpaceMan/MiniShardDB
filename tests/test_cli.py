import pytest

import cli
from models import User


@pytest.fixture
def query_environment(tmp_path, monkeypatch):
    class Gate:
        def __init__(self, path):
            pass

        def login(self):
            return User("bob", "analyst")

        def allow(self, user, query):
            return True

        def audit(self, *args, **kwargs):
            pass

    monkeypatch.setattr(cli, "AuthGate", Gate)
    monkeypatch.setattr(cli.config, "NUM_SHARDS", 2)
    monkeypatch.setattr(cli.config, "SHARD_DIR", tmp_path)
    return tmp_path


def test_query_requires_existing_shards(query_environment):
    with pytest.raises(SystemExit, match="run gen_data.py first"):
        cli.run_query("SELECT * FROM orders")
    assert not list(query_environment.iterdir())


def test_query_reuses_filters_and_prints_grouped_values(query_environment, monkeypatch, capsys):
    closed = []

    class FakeShard:
        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append(self.path)

    class FakeCoordinator:
        def __init__(self, shards):
            self.shards = shards

        def route(self, where):
            return self.shards

        def run(self, sql, params, where):
            return {"pending": 42.0}

    for index in range(2):
        cli.config.shard_path(index).touch()
    monkeypatch.setattr(cli, "Shard", FakeShard)
    monkeypatch.setattr(cli, "Coordinator", FakeCoordinator)
    cli.run_query("SELECT status, SUM(amount) FROM orders GROUP BY status")
    assert "('pending', 42.0)" in capsys.readouterr().out
    assert len(closed) == 2


def test_query_closes_previous_shards_if_initialization_fails(query_environment, monkeypatch):
    closed = []

    class FakeShard:
        def __init__(self, path):
            self.path = path
            if path.endswith("shard1.db"):
                raise RuntimeError("broken filter")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append(self.path)

    for index in range(2):
        cli.config.shard_path(index).touch()
    monkeypatch.setattr(cli, "Shard", FakeShard)
    with pytest.raises(RuntimeError, match="broken filter"):
        cli.run_query("SELECT * FROM orders")
    assert closed == [str(cli.config.shard_path(0))]
