import random
import sqlite3
import sys
from contextlib import ExitStack, closing
from datetime import datetime

import pytest

import config
import gen_data
from models import CustomerRecord, GeneratedData, GenerationOptions, OrderRecord


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gen_data.py"])

    assert gen_data.parse_args() == GenerationOptions(
        num_orders=1000,
        customer_max_orders=5,
        seed=0,
        rebuild_bloom=False,
        fresh=False,
        batch_size=10000,
        workers=None,
    )


def test_parse_args_explicit_options(monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "gen_data.py", "--num-orders", "17", "--customer-max-orders", "3",
        "--seed", "-7", "--batch-size", "4", "--workers", "2",
        "--rebuild-bloom", "--fresh",
    ])

    # Parsing the destructive flag must not execute it.
    assert gen_data.parse_args() == GenerationOptions(17, 3, -7, True, True, 4, 2)


@pytest.mark.parametrize("flag", [
    "--num-orders", "--customer-max-orders", "--batch-size", "--workers",
])
@pytest.mark.parametrize("value", ["0", "-1"])
def test_parse_args_rejects_nonpositive_values(monkeypatch, capsys, flag, value):
    monkeypatch.setattr(sys, "argv", ["gen_data.py", flag, value])

    with pytest.raises(SystemExit) as error:
        gen_data.parse_args()

    assert error.value.code == 2
    assert f"{flag} must be greater than zero" in capsys.readouterr().err


def test_generate_data_batch_preserves_seed_and_call_order(monkeypatch):
    calls = []
    timestamp = datetime(2026, 1, 2, 3, 4, 5)

    class FakeFaker:
        def seed_instance(self, seed):
            calls.append(("seed", seed))

        def name(self):
            calls.append("name")
            return "Customer"

        def email(self):
            calls.append("email")
            return "customer@example.com"

        def date_time_this_year(self):
            calls.append("date")
            return timestamp

    monkeypatch.setattr(gen_data, "Faker", FakeFaker)
    rng = random.Random(23)
    expected_orders = tuple(
        OrderRecord(
            order_id,
            customer_id,
            round(rng.uniform(config.ORDER_AMOUNT_MIN, config.ORDER_AMOUNT_MAX), 2),
            timestamp.isoformat(),
            rng.choice(config.ORDER_STATUSES),
        )
        for order_id, customer_id in [(10, 4), (11, 4), (12, 8)]
    )

    assert gen_data.generate_data_batch((23, [(4, 10, 2), (8, 12, 1)])) == GeneratedData(
        (
            CustomerRecord(4, "Customer", "customer@example.com"),
            CustomerRecord(8, "Customer", "customer@example.com"),
        ),
        expected_orders,
    )
    assert calls == [("seed", 23), "name", "email", "date", "date", "name", "email", "date"]


def test_generate_data_batch_accepts_empty_specs():
    assert gen_data.generate_data_batch((0, [])) == GeneratedData((), ())


@pytest.mark.parametrize("batch_size, expected_commits", [(1, 5), (2, 3), (10, 1)])
def test_insert_in_batches_commits_each_chunk(tmp_path, batch_size, expected_commits):
    path = tmp_path / "batches.db"
    rows = [(index,) for index in range(5)]
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE items (id INTEGER)")
        statements = []
        connection.set_trace_callback(statements.append)

        gen_data.insert_in_batches(connection, "INSERT INTO items VALUES (?)", rows, batch_size)

        assert statements.count("COMMIT") == expected_commits
        assert not connection.in_transaction
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute("SELECT * FROM items ORDER BY id").fetchall() == rows


def test_insert_in_batches_empty_rows_do_not_commit():
    with closing(sqlite3.connect(":memory:")) as connection:
        statements = []
        connection.set_trace_callback(statements.append)

        gen_data.insert_in_batches(connection, "INSERT INTO missing VALUES (?)", [], 2)

        assert statements == []


@pytest.mark.parametrize("batch_size", [1, 4, 20])
def test_generate_and_shard_uses_only_temporary_databases(monkeypatch, tmp_path, batch_size):

    monkeypatch.setattr(gen_data, "BASE_DB", tmp_path / "base.db")
    monkeypatch.setattr(config, "SHARD_DIR", tmp_path / "shards")
    monkeypatch.setattr(config, "NUM_SHARDS", 3)
    config.SHARD_DIR.mkdir()
    options = GenerationOptions(17, 3, 11, False, False, batch_size, 2)
    mapped_batches = []
    worker_counts = []
    batch_writes = {}

    class RecordingShard(gen_data.shard.Shard):
        def insert_customers(self, rows):
            batch_writes.setdefault((self.path, "customers"), []).append(tuple(rows))
            super().insert_customers(rows)

        def insert_orders(self, rows):
            batch_writes.setdefault((self.path, "orders"), []).append(tuple(rows))
            super().insert_orders(rows)

    monkeypatch.setattr(gen_data.shard, "Shard", RecordingShard)

    class InlinePool:
        def __init__(self, max_workers):
            worker_counts.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            pass

        def map(self, function, batches):
            mapped_batches.extend(batches)
            return map(function, batches)

    monkeypatch.setattr(gen_data, "ProcessPoolExecutor", InlinePool)
    # generate_and_shard receives the global RNG state initialized by main.
    with monkeypatch.context() as rng_patch:
        rng_patch.setattr(gen_data.random, "randint", random.Random(options.seed).randint)
        with ExitStack() as resources:
            gen_data.generate_and_shard(options, resources)

    rng = random.Random(options.seed)
    expected_specs = []
    remaining = options.num_orders
    next_order_id = 1
    while remaining:
        count = rng.randint(1, min(options.customer_max_orders, remaining))
        expected_specs.append((len(expected_specs) + 1, next_order_id, count))
        next_order_id += count
        remaining -= count
    generation_batch_size = max(1, batch_size // options.customer_max_orders)
    assert mapped_batches == [
        (options.seed + start // generation_batch_size,
         expected_specs[start:start + generation_batch_size])
        for start in range(0, len(expected_specs), generation_batch_size)
    ]
    assert worker_counts == [2]

    with closing(sqlite3.connect(gen_data.BASE_DB)) as connection:
        customers = connection.execute("SELECT * FROM customers ORDER BY cust_id").fetchall()
        orders = connection.execute("SELECT * FROM orders ORDER BY order_id").fetchall()
        assert connection.execute("PRAGMA foreign_key_list(orders)").fetchone()[2:5] == (
            "customers", "cust_id", "cust_id",
        )
    assert [row[0] for row in customers] == list(range(1, len(expected_specs) + 1))
    assert [row[0] for row in orders] == list(range(1, options.num_orders + 1))
    assert all(config.ORDER_AMOUNT_MIN <= row[2] <= config.ORDER_AMOUNT_MAX for row in orders)
    assert all(row[4] in config.ORDER_STATUSES for row in orders)
    for customer_id, first_order_id, count in expected_specs:
        assert [row[0] for row in orders if row[1] == customer_id] == list(
            range(first_order_id, first_order_id + count)
        )

    for shard_id in range(config.NUM_SHARDS):
        path = config.shard_path(shard_id)
        assert path.is_relative_to(tmp_path)
        with gen_data.shard.Shard(str(path)) as current:
            shard_customers = [row for row in customers if config.shard_of(row[0]) == shard_id]
            shard_orders = [row for row in orders if config.shard_of(row[1]) == shard_id]
            assert current.execute("SELECT * FROM customers ORDER BY cust_id", ()) == shard_customers
            assert current.execute("SELECT * FROM orders ORDER BY order_id", ()) == shard_orders
            assert config.bloom_path(path).exists() == bool(shard_orders)
            assert all(current.bloom_has(row[0]) for row in shard_orders)
            for table, rows in [("customers", shard_customers), ("orders", shard_orders)]:
                assert batch_writes.get((str(path), table), []) == [
                    tuple(rows[start:start + batch_size])
                    for start in range(0, len(rows), batch_size)
                ]
