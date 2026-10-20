# MiniShardDB
> A miniature distributed relational DBMS with Bloom-filter query routing, parallel execution, and RBAC security
---

## What it is

MiniShardDB horizontally fragments the `Orders` and `Customers` relations across 8 SQLite shards using hash partitioning on `cust_id`. The benchmark dataset scales to 1M orders. A **coordinator** layer gives clients *fragmentation and location transparency*: supported SQL queries do not expose shard locations. The current vertical slice routes point queries on `order_id` with per-shard Bloom filters, executes selected shards in parallel, merges grouped results, and applies an **RBAC gate** (admin / analyst / auditor) before execution.

## Current status

The implemented system includes the following:

- Data generation creates a reproducible centralized `data/base.db`, loads
  eight shards, and builds per-shard Bloom filters.
- `Coordinator.route` handles `cust_id`, `order_id`, and unknown predicates.
- `Coordinator.merge` flattens normal results and combines grouped numeric
  results.
- The Bloom filter, parser, routing, flattening, grouped-merge, and security
  tests are present and cover the implemented vertical slice.
- `Coordinator.run` executes non-grouped `order_id` lookups directly on the
  selected shards' existing SQLite connections, avoiding process startup and
  dispatch overhead. Scans, customer queries, and grouped queries still use
  `executor.parallel`; both paths merge the per-shard result lists.
- `parser.py` validates the supported single-statement `SELECT` forms and
  separates SQL structure from numeric parameter values.
- `security.py` implements login, role-based permission checks, and an audit
  API that persists records in `data/audit.db`; these behaviors are covered by
  security tests.
- Supported CLI `SELECT` queries are validated as single statements and numeric
  predicates are passed to SQLite as parameters. Injection payloads are rejected
  before shard execution.
- `cli.py` prompts for a username, checks the query against the user's role,
  audits permitted and denied queries, and runs permitted queries. It also
  supports `--audit` for administrators and auditors. Password validation is
  not implemented yet.
- `database.py` shares customer/order schema creation and read-only SQLite
  connection setup across the generator, workers, and experiments. Callers
  retain ownership of transactions and connection cleanup.
- `models.py` is the single source of truth for shared typed contracts:
  `ParsedQuery`, `User`, generated customer/order records, serializable shard
  jobs, and the named SQL row/result aliases used across the application.
- Centralized SQLite was faster for cold execution because MiniShardDB incurred process-pool and multi-connection startup overhead. After pool reuse, MiniShardDB benefited from concurrent shard scans and outperformed the centralized baseline. Larger datasets are expected to reduce the relative impact of startup overhead, but ultimate scaling will depend on whether the workload is CPU-bound or limited by shared storage bandwidth.
Bloom-filter, routing, parallelism, and injection evidence is checked into
`report/evidence/`. The report, demo video, and final performance-target
interpretation still need to be assembled from those measurements.

## Problem being addressed

On a centralized single-node store:
- Point lookups on non-partitioning columns degrade to **full-relation scans** (I/O grows linearly with table size).
- Large aggregations are **capped by single-threaded execution**, one process must read every page.

## End goal

A working, demoable system **plus quantitative proof** that the advanced (distributed) approach beats the conventional (centralized) one:

| Metric | Target |
|---|---|
| `order_id` point query (1M rows) | ~87% fewer pages read, ≥5× faster |
| Parallel aggregate speedup | 4–6× at 8 workers (measured at 1/2/4/8) |
| Bloom false-positive rate | ≤ 1% |
| Routing precision (100-query suite) | ≥ 99% |
| SQL injection payloads blocked | 10/10 in safe mode |

**Final deliverables:** GitHub repo (this), 8–12 page report, 3–4 min demo video, experiment figures, `decisions.md` design log.

## Architecture

Both relations are partitioned by `cust_id`: an order and its customer are stored
on the same shard, so customer/order joins can be evaluated locally without
replicating the full `Customers` relation to every shard. The centralized
`base.db`
contains the complete dataset and is used as the comparison baseline; the shard
databases are derived copies.

## Stack

Python 3.10+ stdlib (`sqlite3`, `multiprocessing`, `pickle`), `matplotlib` (plots), `pytest` (tests).

## Quickstart: implemented data pipeline

```bash
python -m pytest -q         # run the test suite
python gen_data.py --fresh  # generate 1K orders by default and build 8 shards
```

Data generation uses worker processes for Faker and keeps SQLite writes in the
parent process, which is the only writer. Generated rows are committed in
`--batch-size` transactions; the same option controls shard-loading batches.
`--workers` controls the number of Faker worker processes and defaults to at
most eight workers.

```bash
python gen_data.py --num-orders 1000000 --fresh --batch-size 10000 --workers 4
```

Do not run two `gen_data.py --fresh` commands against the same `data/` directory
at the same time. They delete and recreate the same SQLite and Bloom-filter
files.

The query CLI is available for supported SQL statements. It prompts for one of
the users created in `data/meta.db`: `amal` (admin), `bob` (analyst), or
`carol` (auditor). The current login flow checks the username only; it does not
validate a password.

```bash
python cli.py "SELECT * FROM orders WHERE cust_id = 42"
python cli.py "SELECT * FROM orders WHERE order_id = 777"
python cli.py --audit
```

The audit view is available to `amal` and `carol`; `bob` is denied. Login
currently checks the username only and does not validate a password. This is a
local RBAC demonstration, not production authentication.

Database paths resolve relative to the project directory, not the shell's
working directory. Query workers open existing shard files read-only, and
`Shard.execute()` accepts only `SELECT` statements. The CLI
reuses saved Bloom filters rather than rebuilding them on each query; missing
filters fall back to scanning. Inserting orders through `Shard.insert_orders`
invalidates the affected filter, which can then be rebuilt with `build_bloom()`.
Direct external database writes are not tracked: rebuild filters after those
changes. Bloom files use pickle and must only come from trusted local generation.

`.gitignore` excludes local databases, environment files, Python caches, and
the entire `report/` directory. Already tracked report files remain tracked.
Shareable `.env.example` files are
allowed; never put real credentials in them.

To verify that generated rows are on the correct shards:

```bash
python experiments/verify_shards.py
```

This writes `report/evidence/shard-verification.csv`. The checked-in sample
shows zero misplaced orders and customers on all eight shards.

To regenerate the security evidence:

```bash
python experiments/verify_security.py
```

This writes `report/evidence/injection-results.csv`, comparing ten deliberately
unsafe payloads with the supported-query parser. The safe parser blocks all ten.

The performance and Bloom-filter evidence scripts are:

```bash
python experiments/verify_perf.py
python experiments/fp_rate_charts.py
```

They write CSV evidence and figures under `report/evidence/` and
`report/figures/`.

To regenerate the shard-count and dataset-size charts from the existing
measurements:

```bash
python experiments/benchmark_matrix.py
```

By default, this loads `report/evidence/scaling-matrix.csv` and only rebuilds
the two PNG charts. Pass `--fresh` to delete that CSV, rerun all benchmark
measurements, and rebuild both charts:

```bash
python experiments/benchmark_matrix.py --fresh
```

The default matrix covers 100K, 1M, 5M, and 10M orders; 2, 4, 8, and 16
shards; and 1, 2, 4, and 8 workers. It performs 10 repetitions for every
configuration and writes raw measurements to
`report/evidence/scaling-matrix.csv`. It generates
`report/figures/scaling-matrix-cold.png` and
`report/figures/scaling-matrix-warm.png`. Each subplot represents one
row-count/shard-count pair; bars show the median and whiskers show the
inlier range after robust 3.5-MAD outlier rejection. Raw repetitions remain
available in the CSV but are not plotted as points, preventing timing spikes
from compressing the chart. Each subplot title reports the actual time
represented by its `1.0×` baseline in milliseconds and seconds. Values are
normalized within each subplot to the
1-worker median (`1.0`), used as the base-db-equivalent reference. The y-axis
scale is shared across the full matrix. Purple marks the fastest worker count,
followed by progressively darker grey bars.

The matrix uses a grouped aggregation, so Bloom filters are not involved in
that measurement. Bloom-filter sizing is already calculated from the number of
keys in each filter, so the 10M-row case does not require a special Bloom
implementation.

Matrix dataset creation is batched and uses a process pool for deterministic
order generation; SQLite writes remain in the parent process.

---

## Next steps

### Phase 0 - Cleanup & config
- [x] Repo structure, venv, pytest running
- [x] `bloom.py` implemented, `tests/test_bloom.py` passing
- [x] Move `parse_where` out of `cli.py` -> `parser.py`
- [x] Implement grouped-result merging in `Coordinator.merge`
- [x] Fix `executor.parallel`: pass `s.path` (not Shard objects), return per-shard result lists
- [x] `config.py`: shard paths, Bloom settings, data-generation defaults, worker cap, and `shard_of(cust_id)` - single source of truth
- [x] `decisions.md`: config-as-source-of-truth
- [x] Add `.gitignore` (`.venv/`, `__pycache__/`, `data/`, `*.bloom`) + first push

### Phase 1 - Week 1: vertical slice (end-to-end)
- [x] `gen_data.py` - seeded Customers + Orders, assigns shards via `shard_of`
- [x] `Shard` bodies: `create`, `insert_orders`, `build_bloom` (guard n=0, pickle to `.bloom`), `execute`, `bloom_has`
- [x] Verify and document that each shard contains only `cust_id % 8 == shard_number` rows
- [x] `Coordinator.route` - `cust_id` case, then `order_id`/Bloom case
- [x] **End-to-end point query through `cli.py` works - screenshot**
- [x] Aggregates: GROUP BY merge across shards (unit-tested)
- [x] Add end-to-end tests for `AuthGate.login`, `allow`, and `audit`
- [x] Parser tests for table extraction, plus routing and merge tests

### Phase 2 - Week 2: parallelism
- [x] `executor.parallel` worker helper
- [x] Parallel path in `Coordinator.run` (use `executor.parallel`)
- [x] Demonstrate a local join (Customers and Orders are co-located by `cust_id`)
- [x] Scale data to 1M rows
- [x] Add `experiments/verify_perf.py` with cold-start, warm-pool, and centralized timing modes
- [x] **E2:** run the final 1/2/4/8-worker benchmark and generate the speedup plot
- [x] **E3:** benchmark MiniShardDB against the single centralized SQLite file and generate the comparison figure

### Phase 3 - Week 3: security
- [x] Add a supported-query parser that separates SQL structure from user-provided values
- [x] Parameterize extracted values with SQLite placeholders before shard execution
- [x] **E4:** 10 injection payloads × {vulnerable, safe} results table
- [x] Add CLI support for auditors to read the audit log

### Phase 4 - Week 4: evaluation & wrap
- [x] **E1:** Bloom FP-rate curve vs. theory + routing precision
- [x] Re-run all experiments on final data → `report/figures/`
- [ ] Report (8–12 pages)
- [ ] Demo video (3–4 min, scripted)
- [ ] Viva prep: why Bloom with a partition map? shard failure during a query?
