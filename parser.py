import re

def parse_where(q: str) -> tuple[str, int] | None:
    """Parse a SQL query string to extract the first WHERE clause condition in the form of (column, value). Returns None if no WHERE clause is found."""
    m = re.search(r"WHERE\s+(\w+)\s*=\s*(\d+)", q, re.I)
    return (m.group(1), int(m.group(2))) if m else None

def has_group_by(q: str) -> bool:
    """Check if a SQL query string contains a GROUP BY clause."""
    return re.search(r"\bGROUP\s+BY\b", q, re.IGNORECASE) is not None

def parse_table(sql: str) -> str | None:
    """Return the first table referenced by a supported SQL statement.

    The authorization layer only needs the table name, not a full SQL parser.
    ``FROM`` covers SELECT statements, while the other keywords cover common
    write statements and joins.
    """
    match = re.search(
        r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        sql,
        re.IGNORECASE,
    )
    return match.group(1).lower() if match else None
