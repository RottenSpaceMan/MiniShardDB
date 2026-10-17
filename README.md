# MiniShardDB
> A miniature distributed relational DBMS with Bloom-filter query routing, parallel execution, and RBAC security
---

## What it is

MiniShardDB horizontally fragments the `Orders` and `Customers` relations across 8 SQLite shards using hash partitioning on `cust_id`. The final benchmark target is 1M orders. A **coordinator** layer gives clients *fragmentation & location transparency*: they write ordinary SQL and never know the data is split. The current vertical slice routes point queries on `order_id` with per-shard Bloom filters, executes selected shards in parallel, merges grouped results, and applies an **RBAC gate** (admin / analyst / auditor) before execution.

## Current status

The current working tree has the data-generation, query-routing, and parallel
execution vertical slice in place:

- Data generation creates a reproducible centralized `data/base.db`, loads
  eight shards, and builds per-shard Bloom filters.
- `Coordinator.route` handles `cust_id`, `order_id`, and unknown predicates.
- `Coordinator.merge` flattens normal results and combines grouped numeric
  results.
- The Bloom filter, parser, routing, flattening, grouped-merge, and security
  tests are present and cover the implemented vertical slice.
- `Coordinator.run` dispatches selected shard database paths through
  `executor.parallel` and merges the per-shard result lists.
- `parser.parse_table` now extracts the first referenced table for the
  authorization layer, and parser tests cover the supported statement forms.
- `security.py` implements login, role-based permission checks, and an audit
  API that persists records in `data/audit.db`; these behaviors are covered by
  security tests.
- Supported CLI `SELECT` queries are validated as single statements and numeric
  predicates are passed to SQLite as parameters. Injection payloads are rejected
  before shard execution.
- `cli.py` prompts for a username, checks the query against the user's role,
  audits permitted and denied queries, and runs permitted queries. Password
  validation and CLI audit-log viewing are not implemented yet.
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

```
 SQL --> AuthGate (RBAC check + audit log)
              |
              v
        Coordinator (parser --> route)
              │   cust_id  → partition map (cust_id % 8)
              │   order_id → Bloom filter probes
              v
        executor (multiprocessing.Pool)
shard0, shard1, shard2, …shard7   (SQLite files + .bloom)
              v
        Coordinator.merge --> result
```

Both relations are partitioned by `cust_id`: an order and its customer are stored
on the same shard, so customer/order joins can be evaluated locally without
replicating the full `Customers` relation to every shard. The centralized
`base.db`
contains the complete dataset and is used as the comparison baseline; the shard
databases are derived copies.

## Concepts → syllabus map

| Module | Concept | Where |
|---|---|---|
| M1 | Selection cost, cost-based shard choice | `coordinator.route`, E3 |
| M2 | RBAC, SQL-injection defense | `security.py`, parameterized executor |
| M3 | Partitioning, parallel query processing | `shard.py`, `executor.py` |
| M4 | Horizontal fragmentation, transparency, Bloom filters, distributed query processing | `coordinator.py`, `bloom.py` |

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
```

To verify that generated rows are on the correct shards:

```bash
python experiments/verify_shards.py
```

This writes `report/evidence/shard-verification.csv`. The checked-in sample
shows zero misplaced orders and customers on all eight shards.

---

## Next steps

### Phase 0 - Cleanup & config
- [x] Repo structure, venv, pytest running
- [x] `bloom.py` implemented, `tests/test_bloom.py` passing
- [x] Move `parse_where` out of `cli.py` -> `parser.py`
- [x] Implement grouped-result merging in `Coordinator.merge`
- [x] Fix `executor.parallel`: pass `s.path` (not Shard objects), return per-shard result lists
- [x] `config.py`: `NUM_SHARDS`, `SHARD_DIR`, `shard_of(cust_id)` - single source of truth
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
- [ ] Add CLI support for auditors to read the audit log

### Phase 4 - Week 4: evaluation & wrap
- [x] **E1:** Bloom FP-rate curve vs. theory + routing precision
- [x] Re-run all experiments on final data → `report/figures/`
- [ ] Report (8–12 pages) + polish this README
- [ ] Demo video (3–4 min, scripted)
- [ ] Viva prep: why Bloom with a partition map? shard failure during a query?
