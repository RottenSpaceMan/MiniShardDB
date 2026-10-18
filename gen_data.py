import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack, closing
import os
import random
from pathlib import Path
import sqlite3
from collections.abc import Sequence
import time

from faker import Faker

import config
import shard
from models import (
    CustomerRecord,
    CustomerSpec,
    GeneratedData,
    GenerationOptions,
    OrderRecord,
    SqlRow,
)


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_DB = PROJECT_ROOT / config.BASE_DB


def parse_args() -> GenerationOptions:
    parser = argparse.ArgumentParser(description="Generate Customer and Order data.")
    parser.add_argument("--num-orders", type=int, default=1000, help="Number of orders to generate.")
    parser.add_argument("--customer-max-orders", type=int, default=5, help="Maximum number of orders per customer.")
    parser.add_argument("--seed", type=int, default=0, help="Random seed for reproducibility.")
    parser.add_argument("--rebuild-bloom", action="store_true", help="Rebuild Bloom filters for all shards.")
    parser.add_argument("--fresh", action="store_true", help="Delete existing base.db and shard files before generating new data.")
    parser.add_argument("--batch-size", type=int, default=10000, help="Rows per SQLite insert transaction during generation and sharding.")
    parser.add_argument("--workers", type=int, default=None, help="Number of worker processes used for Faker generation.")
    args = parser.parse_args()

    if args.num_orders < 1:
        parser.error("--num-orders must be greater than zero")
    if args.customer_max_orders < 1:
        parser.error("--customer-max-orders must be greater than zero")
    if args.batch_size < 1:
        parser.error("--batch-size must be greater than zero")
    if args.workers is not None and args.workers < 1:
        parser.error("--workers must be greater than zero")
    return GenerationOptions(
        num_orders=args.num_orders,
        customer_max_orders=args.customer_max_orders,
        seed=args.seed,
        rebuild_bloom=args.rebuild_bloom,
        fresh=args.fresh,
        batch_size=args.batch_size,
        workers=args.workers,
    )


def generate_data_batch(
    batch: tuple[int, list[CustomerSpec]],
) -> GeneratedData:
    """Generate one independent Faker batch in a worker process."""
    batch_seed, customer_specs = batch
    fake = Faker()
    fake.seed_instance(batch_seed)
    rng = random.Random(batch_seed)
    customers: list[CustomerRecord] = []
    orders: list[OrderRecord] = []

    for customer_id, first_order_id, order_count in customer_specs:
        customers.append(
            CustomerRecord(customer_id, fake.name(), fake.email())
        )
        for offset in range(order_count):
            orders.append(
                OrderRecord(
                    order_id=first_order_id + offset,
                    cust_id=customer_id,
                    amount=round(
                        rng.uniform(
                            config.ORDER_AMOUNT_MIN,
                            config.ORDER_AMOUNT_MAX,
                        ),
                        2,
                    ),
                    created_at=fake.date_time_this_year().isoformat(),
                    status=rng.choice(config.ORDER_STATUSES),
                ),
            )

    return GeneratedData(tuple(customers), tuple(orders))

def insert_in_batches(
    connection: sqlite3.Connection,
    sql: str,
    rows: Sequence[SqlRow],
    batch_size: int,
) -> None:
    for start in range(0, len(rows), batch_size):
        connection.executemany(sql, rows[start:start + batch_size])
        connection.commit()


def remove_existing_databases() -> None:
    if BASE_DB.exists():
        print(f"Deleting existing base database: {BASE_DB}")
        BASE_DB.unlink()
    for i in range(config.NUM_SHARDS):
        shard_path = PROJECT_ROOT / config.shard_path(i)
        print(f"Deleting existing shard file: {shard_path}", end="")
        if os.path.exists(shard_path):
            os.remove(shard_path)
        bloom_path = config.bloom_path(shard_path)
        if os.path.exists(bloom_path):
            print(f" and its Bloom filter: {bloom_path}", end="")
            os.remove(bloom_path)
    print()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    (PROJECT_ROOT / config.SHARD_DIR).mkdir(parents=True, exist_ok=True)
    if args.fresh:
        remove_existing_databases()

    with ExitStack() as resources:
        generate_and_shard(args, resources)


def generate_and_shard(args: GenerationOptions, resources: ExitStack) -> None:
    """Generate and shard customer and order data."""
    con = resources.enter_context(closing(sqlite3.connect(BASE_DB)))
    cur = con.cursor()

    print("Creating and initializing base tables...")
    start_time = time.perf_counter()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    cust_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL
                )
                """)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    cust_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    FOREIGN KEY (cust_id) REFERENCES customers(cust_id)
                )
                """)

    remaining_orders = args.num_orders
    customer_id = 1
    next_order_id = 1
    customer_specs = []
    while remaining_orders > 0:
        num_orders_for_customer = random.randint(
            1, min(args.customer_max_orders, remaining_orders)
        )
        customer_specs.append((customer_id, next_order_id, num_orders_for_customer))
        next_order_id += num_orders_for_customer
        remaining_orders -= num_orders_for_customer
        customer_id += 1

    generation_batch_size = max(1, args.batch_size // args.customer_max_orders)
    generation_batches = [
        (
            args.seed + batch_start // generation_batch_size,
            customer_specs[batch_start:batch_start + generation_batch_size],
        )
        for batch_start in range(0, len(customer_specs), generation_batch_size)
    ]

    workers = args.workers or min(config.DEFAULT_MAX_WORKERS, os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for generated in pool.map(generate_data_batch, generation_batches):
            insert_in_batches(
                con,
                "INSERT INTO customers (cust_id, name, email) VALUES (?, ?, ?)",
                [customer.as_row() for customer in generated.customers],
                args.batch_size,
            )
            insert_in_batches(
                con,
                "INSERT INTO orders (order_id, cust_id, amount, created_at, status) "
                "VALUES (?, ?, ?, ?, ?)",
                [order.as_row() for order in generated.orders],
                args.batch_size,
            )
    end_time = time.perf_counter()

    print(f"Number of customers generated: {customer_id - 1}")
    print(f"Number of orders generated: {next_order_id - 1}")
    print(f"Data generation time: {end_time - start_time:.2f} seconds")
    print("Base data generation complete. Sharding...")

    start_time = time.perf_counter()
    shards = []
    for i in range(config.NUM_SHARDS):
        current_shard = shard.Shard(
            str(PROJECT_ROOT / config.shard_path(i)),
            rebuild_bloom=args.rebuild_bloom,
        )
        resources.callback(current_shard.close)
        shards.append(current_shard)
        current_shard.create()

    customer_batches = defaultdict(list)
    for cust in cur.execute("SELECT * FROM customers"):
        shard_index = config.shard_of(cust[0])
        batch = customer_batches[shard_index]
        batch.append(cust)
        if len(batch) >= args.batch_size:
            shards[shard_index].insert_customers(batch)
            batch.clear()

    for shard_index, batch in customer_batches.items():
        if batch:
            shards[shard_index].insert_customers(batch)

    order_batches = defaultdict(list)
    for order in cur.execute("SELECT * FROM orders"):
        shard_index = config.shard_of(order[1])
        batch = order_batches[shard_index]
        batch.append(order)
        if len(batch) >= args.batch_size:
            shards[shard_index].insert_orders(batch)
            batch.clear()

    for shard_index, batch in order_batches.items():
        if batch:
            shards[shard_index].insert_orders(batch)

    end_time = time.perf_counter()
    print(f"Sharding time: {end_time - start_time:.2f} seconds")

    print("Sharding complete. Building Bloom filters...")
    for current_shard in shards:
        if args.rebuild_bloom or not config.bloom_path(current_shard.path).exists():
            current_shard.build_bloom()
    print("Bloom filters built. Data generation and sharding complete.")
    for current_shard in shards:
        print(
            f"Shard {current_shard.path} has "
            f"{current_shard.cur.execute('SELECT COUNT(*) FROM orders').fetchone()[0]} "
            f"orders and "
            f"{current_shard.cur.execute('SELECT COUNT(*) FROM customers').fetchone()[0]} "
            f"customers."
        )



if __name__ == "__main__":
    main()
