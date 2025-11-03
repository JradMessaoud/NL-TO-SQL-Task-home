"""Test the DB helper functions for sanitization and query execution."""
import pytest
import sqlite3
from db import is_safe_select, execute_select
from config import DB_PATH


def test_safe_selects():
    """Test SQL sanitizer with various SELECT statements."""
    safe_queries = [
        "SELECT * FROM patients",
        "SELECT name, age FROM patients WHERE age > 60",
        "SELECT d.name, COUNT(*) FROM doctors d JOIN appointments a",
        "(SELECT DISTINCT p.name FROM patients p)",
        "  SELECT  *  FROM  patients  ",  # Extra spaces
    ]
    for sql in safe_queries:
        ok, _ = is_safe_select(sql)
        assert ok, f"Should allow: {sql}"


def test_unsafe_queries():
    """Test SQL sanitizer blocks unsafe statements."""
    unsafe_queries = [
        "DELETE FROM patients",
        "SELECT * FROM patients; DROP TABLE patients",
        "INSERT INTO patients VALUES (1, 'Bob', 30)",
        "UPDATE patients SET age = 31",
        "SELECT * FROM patients; -- comment",
        "/* comment */ SELECT * FROM patients",
        "PRAGMA table_info(patients)",
        "BEGIN TRANSACTION",
        "CREATE TABLE test (id INTEGER)",
    ]
    for sql in unsafe_queries:
        ok, _ = is_safe_select(sql)
        assert not ok, f"Should block: {sql}"


def test_execute_select(tmp_path):
    """Test query execution with a temporary database."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    
    # Create a test table
    cur.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
    cur.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
    conn.commit()
    conn.close()
    
    # Test various valid queries
    cols, rows = execute_select(str(db), "SELECT * FROM test")
    assert cols == ["id", "val"]
    assert len(rows) == 2
    
    cols, rows = execute_select(str(db), "SELECT val FROM test WHERE id = ?", (1,))
    assert cols == ["val"]
    assert rows == [("one",)]
    
    # Test invalid queries
    with pytest.raises(ValueError):
        execute_select(str(db), "DELETE FROM test")
    
    with pytest.raises(ValueError):
        execute_select(str(db), "SELECT * FROM nonexistent")