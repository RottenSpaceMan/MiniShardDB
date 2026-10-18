import csv
import sqlite3
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
import config


def verify_shards() -> list[dict[str, int]]:
    rows = []

    for shard_id in range(config.NUM_SHARDS):
        path = config.shard_path(shard_id)

        with sqlite3.connect(path) as connection:
            order_rows = connection.execute(
                "SELECT cust_id FROM orders"
            ).fetchall()
            customer_rows = connection.execute(
                "SELECT cust_id FROM customers"
            ).fetchall()

        wrong_orders = sum(
            cust_id % config.NUM_SHARDS != shard_id
            for (cust_id,) in order_rows
        )
        wrong_customers = sum(
            cust_id % config.NUM_SHARDS != shard_id
            for (cust_id,) in customer_rows
        )

        rows.append(
            {
                "shard": shard_id,
                "orders": len(order_rows),
                "customers": len(customer_rows),
                "wrong_orders": wrong_orders,
                "wrong_customers": wrong_customers,
            }
        )

    return rows


def main() -> None:
    fieldnames = [
        "shard",
        "orders",
        "customers",
        "wrong_orders",
        "wrong_customers",
    ]
    output_path = PROJECT_ROOT / "report" / "evidence" / "shard-verification.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(verify_shards())

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
