06/10 22:01:
- Move hardcoded shard logic to config.py
11/10 20:41:
- Added batch writing to gen_data.py to improve performance by moving overhead from python to SQLite.
- Added Processing Pool to gen_data.py to generate data in parallel.