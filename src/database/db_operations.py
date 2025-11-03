"""Database operations for the Medical Database Query System"""
import sqlite3
import os
from typing import List, Dict, Any

class DatabaseOperations:
    def __init__(self, db_path: str):
        """Initialize database connection"""
        self.db_path = db_path
        
    def check_database_exists(self) -> bool:
        """Check if the database file exists and is a valid SQLite database"""
        if not os.path.exists(self.db_path):
            return False
        try:
            # Try to connect with timeout to avoid locked database issues
            with sqlite3.connect(self.db_path, timeout=5) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return True
        except sqlite3.Error:
            return False

    def execute_query(self, query: str, safe: bool = False) -> List[Dict[str, Any]]:
        """Execute SQL query and return results as list of dictionaries
        
        Args:
            query: The SQL query to execute
            safe: If True, return empty list instead of raising error when table doesn't exist
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(query)
                    return [dict(row) for row in cursor.fetchall()]
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    import time
                    time.sleep(1)  # Wait a second before retrying
                    continue
                if safe and "no such table" in str(e):
                    return []
                raise e

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information"""
        return self.execute_query(f"PRAGMA table_info({table_name})")

    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        return [row['name'] for row in self.execute_query(query)]