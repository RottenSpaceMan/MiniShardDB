import re

def parse_where(q):
    m = re.search(r"WHERE\s+(\w+)\s*=\s*(\d+)", q, re.I)
    return (m.group(1), int(m.group(2))) if m else (None, None)

def parse_table():
    """For AuthGate.allow and has_group_by()"""
