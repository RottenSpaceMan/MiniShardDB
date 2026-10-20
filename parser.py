"""Validate the supported SELECT grammar and extract routing metadata."""

import re

from models import ParsedQuery, ParsedWhere


_SELECT_PATTERN = re.compile(
    r"""
    SELECT\s+(?P<columns>[A-Za-z0-9_.*,\s()+-]+?)\s+
    FROM\s+(?P<table>orders|customers)
    (?:
        \s+JOIN\s+(?P<join_table>orders|customers)\s+
        ON\s+[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*
        \s*=\s*[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*
    )?
    (?:
        \s+WHERE\s+(?P<column>cust_id|order_id)\s*=\s*(?P<value>\d+)
    )?
    (?:
        \s+GROUP\s+BY\s+[A-Za-z_][A-Za-z0-9_.]*
    )?
    (?:
        \s+ORDER\s+BY\s+[A-Za-z_][A-Za-z0-9_.]*(?:\s+(?:ASC|DESC))?
    )?
    \s*
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_supported_query(sql: str) -> ParsedQuery:
    """Validate a supported SELECT and parameterize its numeric predicate."""
    normalized = " ".join(sql.strip().split())
    if (
        not is_single_statement(normalized)
        or re.search(
            r"\b(?:AND|DELETE|DROP|INSERT|OR|PRAGMA|UNION|UPDATE)\b",
            normalized,
            re.IGNORECASE,
        )
    ):
        raise ValueError("Only one SQL statement is allowed")

    match = _SELECT_PATTERN.fullmatch(normalized)
    if match is None:
        raise ValueError("Unsupported or unsafe SELECT query")

    table = match.group("table").lower()
    column = match.group("column")
    value_text = match.group("value")
    where = (column.lower(), int(value_text)) if column and value_text else None

    if where is None:
        return ParsedQuery(normalized, (), table, None)

    parameterized = re.sub(
        rf"\b{re.escape(column)}\s*=\s*{re.escape(value_text)}\b",
        f"{column} = ?",
        normalized,
        count=1,
        flags=re.IGNORECASE,
    )
    return ParsedQuery(parameterized, (where[1],), table, where)


def parse_where(sql: str) -> ParsedWhere:
    """Extract the first supported numeric equality predicate."""
    match = re.search(r"\bWHERE\s+(cust_id|order_id)\s*=\s*(\d+)", sql, re.IGNORECASE)
    if match is None:
        return None
    return match.group(1).lower(), int(match.group(2))


def has_group_by(sql: str) -> bool:
    """Check whether a SQL query contains a GROUP BY clause."""
    return re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE) is not None


def parse_table(sql: str) -> str | None:
    """Return the first table referenced by a SQL statement."""
    match = re.search(
        r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        sql,
        re.IGNORECASE,
    )
    return match.group(1).lower() if match else None


def is_single_statement(sql: str) -> bool:
    """Reject statement chaining and SQL comments before authorization."""
    stripped = sql.strip()
    return bool(stripped) and not any(
        token in stripped for token in (";", "--", "/*", "*/")
    )
