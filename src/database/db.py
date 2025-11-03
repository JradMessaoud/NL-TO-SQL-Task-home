"""Database helper functions: connect, sanitize, execute SELECT-only queries.

This module enforces that only safe SELECT statements are executed. It returns
rows and column names.
"""
import re
import sqlite3
from typing import List, Tuple


_FORBIDDEN = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|PRAGMA|EXEC|REPLACE|TRUNCATE|MERGE|BEGIN|COMMIT|ROLLBACK)\b", re.I)


def is_safe_select(sql: str) -> Tuple[bool, str]:
    """Return (allowed, normalized_sql). Only allow SELECT statements and disallow dangerous keywords.

    The returned SQL is stripped and has a trailing semicolon removed.
    """
    if not sql or not isinstance(sql, str):
        return False, ""
    s = sql.strip()
    # remove trailing semicolons
    if s.endswith(";"):
        s = s[:-1].strip()
    # disallow SQL comments
    if "--" in s or "/*" in s:
        return False, s
    if _FORBIDDEN.search(s):
        return False, s
    # Allow only statements starting with SELECT (optionally with parentheses)
    if re.match(r"^(\(|\s)*SELECT\b", s, re.I):
        return True, s
    return False, s


def execute_select(db_path: str, sql: str, params: Tuple = ()) -> Tuple[List[str], List[Tuple]]:
    """Execute a sanitized SELECT and return (columns, rows).

    Raises ValueError if SQL is not a safe SELECT.
    """
    ok, norm = is_safe_select(sql)
    if not ok:
        raise ValueError("Only safe SELECT statements are allowed")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(norm, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description] if cur.description else []
    conn.close()
    return columns, rows
