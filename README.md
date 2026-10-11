# MiniShardDB
> A miniature distributed relational DBMS with Bloom-filter query routing, parallel execution, and RBAC security
---

## What it is

MiniShardDB is designed to horizontally fragment the `Orders` and `Customers` relations across 8 SQLite shards using hash partitioning on `cust_id`. The final benchmark target is 1M orders. A **coordinator** layer gives clients *fragmentation & location transparency*: they write ordinary SQL and never know the data is split. Point queries on `order_id` use per-shard Bloom filters to avoid scanning most shards; false positives may cause a small number of additional shard probes; analytical aggregations run **in parallel** across shards and merge at the coordinator. All access passes through an **RBAC gate** (admin / analyst / auditor) with a parameterized-query-only execution path and an audit log.

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
| SQL injection payloads blocked | 10/10 (0/10 in vulnerable mode) |

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
| M4 | Horizontal fragmentation, transparency, Bloom filters, distributed query processing, (stretch: 2PC) | `coordinator.py`, `bloom.py` |

## Stack

Python 3.10+ stdlib (`sqlite3`, `multiprocessing`, `pickle`), `matplotlib` (plots), `pytest` (tests).

## Planned Quickstart

```bash
python gen_data.py --fresh  # generate 1K orders, seeded & reproducible; Move to 1M for final benchmark
python shard.py             # create + load 8 shards, build & pickle blooms
python cli.py "SELECT region, SUM(amount) FROM orders WHERE cust_id = 42 GROUP BY region"
python cli.py "SELECT * FROM orders WHERE order_id = 777"     # bloom-routed
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

---

## TODO

### Phase 0 - Cleanup & config
- [x] Repo structure, venv, pytest running
- [x] `bloom.py` implemented, `tests/test_bloom.py` passing
- [x] Move `parse_where` out of `cli.py` -> `parser.py`
- [x] Implement grouped-result merging in `Coordinator.merge`
- [x] Fix `executor.parallel`: pass `s.path` (not Shard objects), return per-shard result lists
- [x] `config.py`: `NUM_SHARDS`, `SHARD_DIR`, `shard_of(cust_id)` - single source of truth
- [x] `decisions.md`: config-as-source-of-truth
- [x] Add `.gitignore` (`.venv/`, `__pycache__/`, `data/`, `*.bloom`) + first push

### Phase 1 - Week 1: vertical slice (serial, end-to-end)
- [x] `gen_data.py` - seeded Customers + Orders, assigns shards via `shard_of`
- [x] `Shard` bodies: `create`, `insert_orders`, `build_bloom` (guard n=0, pickle to `.bloom`), `execute`, `bloom_has`
- [x] Verify: `data/shard3.db` contains only `cust_id % 8 == 3` rows
- [ ] `Coordinator.route` - cust_id case first, then order_id/bloom case
- [ ] **End-to-end point query through `cli.py` works - screenshot**
- [ ] Aggregates: GROUP BY merge across shards
- [ ] `AuthGate` bodies: `login`, `allow`, `audit`; wire into CLI
- [ ] 3+ pytest tests (routing, merge, RBAC deny) + commit

### Phase 2 - Week 2: parallelism
- [ ] Parallel path in `Coordinator.run` (Pool via `executor.parallel`)
- [ ] Parallel local join (Customers and Orders co-located by `cust_id`)
- [ ] Scale data to 1M rows
- [ ] **E2:** speedup plot, 1/2/4/8 workers
- [ ] **E3:** MiniShardDB vs. single centralized SQLite file

### Phase 3 - Week 3: security + stretch
- [ ] **E4:** 10 injection payloads × {vulnerable, safe} results table
- [ ] Auditor role can read the audit log via CLI
- [ ] STRETCH (only if on schedule): 2PC with `--kill-participant` demo

### Phase 4 - Week 4: evaluation & wrap
- [ ] **E1:** Bloom FP-rate curve vs. theory + routing precision
- [ ] Re-run all experiments on final data → `report/figures/`
- [ ] Report (8–12 pages) + polish this README
- [ ] Demo video (3–4 min, scripted)
- [ ] Viva prep: why Bloom with a partition map? shard failure mid-query/mid-commit? 2PC blocking?
