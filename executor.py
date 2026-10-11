import sqlite3
from multiprocessing import Pool


def run_on_shard(args):
    db_path, sql, params = args

    with sqlite3.connect(db_path) as con:
        return con.execute(sql, params).fetchall()


def parallel(targets, sql, params=(), workers=None):
    jobs = [(s.path, sql, params) for s in targets]
    with Pool(workers) as pool:
        return pool.map(run_on_shard, jobs)