06/10 22:01:
- Move hardcoded shard logic to config.py
11/10 20:41:
- Added batch writing to gen_data.py to improve performance by moving overhead from python to SQLite.
- Added Processing Pool to gen_data.py to generate data in parallel.
14/10 11:04:
- Added audit logging to AuthGate and Coordinator, with a new `audit.db` SQLite database.
- Added config.AUDIT_DB constant to config.py.
- Added end-to-end tests for AuthGate.login, allow, and audit.
- Added an experiments/ directory for testing and benchmarking different approaches to sharding, Bloom filters, and SQLite performance.
- Adde verify_shards script to experiments/ to verify that each shard contains only rows with cust_id % NUM_SHARDS == shard_number, writes to csv