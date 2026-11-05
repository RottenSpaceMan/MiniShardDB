# MiniShardDB

A small distributed relational database prototype that partitions SQLite data
across shards, routes queries with Bloom filters, executes work in parallel, and
checks access with role-based permissions.

## Files

| File or directory | Description |
|---|---|
| `config.py` | Shared paths, shard settings, and project defaults. |
| `models.py` | Typed records and contracts shared across the application. |
| `database.py` | SQLite schema creation and connection helpers. |
| `gen_data.py` | Generates sample customers and orders and loads the shards. |
| `shard.py` | Creates shards, executes read queries, and manages Bloom filters. |
| `bloom.py` | Bloom-filter implementation used for query routing. |
| `parser.py` | Validates supported `SELECT` queries and extracts parameters. |
| `coordinator.py` | Routes queries, runs shard work, and merges results. |
| `executor.py` | Runs shard jobs in parallel worker processes. |
| `security.py` | Provides users, roles, permissions, and audit logging. |
| `cli.py` | Command-line interface for queries and audit access. |
| `experiments/` | Benchmarking and verification scripts. |
| `tests/` | Automated unit and integration tests. |
| `data/` | Generated databases and Bloom filters created by the data pipeline. |

## Architecture

`Customers` and `Orders` are hash-partitioned across eight SQLite shards by
`cust_id`, keeping related rows together for local joins. The coordinator hides
shard locations, uses Bloom filters for `order_id` routing, executes selected
shards, and merges the results. `data/base.db` is the centralized comparison
database.

## Stack

Python 3.10+, SQLite, `multiprocessing`, Faker, matplotlib, pandas, and pytest.

## Quick start

1. Install dependencies:

   ```bash
   python -m pip install -r requirements.txt
   ```

2. Run the tests:

   ```bash
   python -m pytest -q
   ```

3. Generate the default dataset and eight shards:

   ```bash
   python gen_data.py --fresh
   ```

4. Run a query:

   ```bash
   python cli.py "SELECT * FROM orders WHERE cust_id = 42"
   python cli.py "SELECT * FROM orders WHERE order_id = 777"
   ```

For a larger dataset, use
`python gen_data.py --num-orders 1000000 --fresh --batch-size 10000 --workers 4`.
The generated `data/` directory is local runtime data and should not be shared
between concurrent `--fresh` runs.
