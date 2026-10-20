"""Compare raw SQLite execution with the supported-query security parser."""

import csv
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from parser import parse_supported_query


PAYLOADS: list[str] = [
    "SELECT * FROM orders WHERE order_id = 1 OR 1=1",
    "SELECT * FROM orders WHERE order_id = 1 OR '1'='1'",
    "SELECT * FROM orders; DROP TABLE orders;",
    "SELECT * FROM orders WHERE order_id = 1; DELETE FROM orders;",
    "SELECT * FROM orders WHERE order_id = 1 -- comment",
    "SELECT * FROM orders UNION SELECT * FROM customers",
    "SELECT * FROM orders WHERE order_id = 1 AND 1=1",
    "SELECT * FROM orders WHERE order_id = 1 /* comment */",
    "SELECT * FROM orders WHERE order_id = 1 OR order_id = 2",
    "SELECT * FROM orders WHERE order_id = 1; PRAGMA user_version;",
]


def vulnerable_result(payload: str) -> str:
    """Run a payload as a deliberately unsafe multi-statement script."""
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE orders (order_id INTEGER)")
        connection.execute("INSERT INTO orders VALUES (1)")
        connection.execute("CREATE TABLE customers (cust_id INTEGER)")
        connection.execute("INSERT INTO customers VALUES (2)")
        try:
            connection.executescript(payload)
        except sqlite3.Error:
            return "blocked"
        return "accepted"


def safe_result(payload: str) -> str:
    """Return whether the supported-query parser accepts the payload."""
    try:
        parse_supported_query(payload)
    except ValueError:
        return "blocked"
    return "accepted"


def main() -> None:
    """Write acceptance results for the unsafe script and supported-query parser."""
    output_path = PROJECT_ROOT / "report" / "evidence" / "injection-results.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=("payload", "vulnerable_result", "safe_result"),
        )
        writer.writeheader()
        for payload in PAYLOADS:
            writer.writerow(
                {
                    "payload": payload,
                    "vulnerable_result": vulnerable_result(payload),
                    "safe_result": safe_result(payload),
                }
            )
    print(f"Wrote {len(PAYLOADS)} results to {output_path}")


if __name__ == "__main__":
    main()
