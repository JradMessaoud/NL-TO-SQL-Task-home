"""Schema definition and helper to create the SQLite medical schema.

This module exposes `SCHEMA_SQL` (ordered list of CREATE TABLE statements) and
`create_schema(conn)` convenience function.
"""
import json
from pathlib import Path
from typing import Dict, List

# Load schema from JSON
SCHEMA_PATH = Path(__file__).parent / "schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA_JSON = json.load(f)

def _build_schema_sql() -> List[str]:
    """Build CREATE TABLE statements from schema.json."""
    sql = []
    for table, info in SCHEMA_JSON["tables"].items():
        cols = []
        for col in info["columns"]:
            spec = [col["name"], col["type"]]
            if col.get("primary_key"):
                spec.append("PRIMARY KEY")
            cols.append(" ".join(spec))
        
        # Add foreign key constraints
        for col in info["columns"]:
            if "references" in col:
                ref = col["references"]
                cols.append(f"FOREIGN KEY({col['name']}) REFERENCES {ref['table']}({ref['column']})")
        
        create_stmt = f"CREATE TABLE IF NOT EXISTS {table} (\n    "
        create_stmt += ",\n    ".join(cols)
        create_stmt += "\n);"
        sql.append(create_stmt)
    return sql

# Generate SQL statements from schema
SCHEMA_SQL: List[str] = _build_schema_sql()

def create_schema(conn) -> None:
    """Create all tables (idempotent).

    Args:
        conn: sqlite3.Connection
    """
    cur = conn.cursor()
    # enable foreign keys enforcement
    cur.execute("PRAGMA foreign_keys = ON;")
    for stmt in SCHEMA_SQL:
        cur.executescript(stmt)
    conn.commit()
