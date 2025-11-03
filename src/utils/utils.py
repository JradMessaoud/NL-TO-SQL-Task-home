"""Shared utility functions for the medical NL→SQL demo."""
import logging
import os
from typing import Any, Dict, Optional
import sqlite3
from pathlib import Path

from config import LOG_DIR, LOG_FILE, LOG_FORMAT, LOG_LEVEL


def setup_logging(name: str) -> logging.Logger:
    """Configure logging with consistent format across modules."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Ensure log directory exists
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(exist_ok=True)
    
    handler = logging.FileHandler(log_dir / LOG_FILE)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    
    return logger


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Get a database connection with foreign keys enabled."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def safe_get_config(key: str, default: Any = None) -> Any:
    """Get configuration value from environment or return default."""
    return os.environ.get(f"MEDICAL_NLP_{key}", default)


def format_sql_error(e: Exception) -> str:
    """Format a SQLite error for user display, removing sensitive details."""
    msg = str(e)
    # Remove any embedded values or stack traces
    if "near " in msg:
        msg = msg.split("near")[0].strip()
    return f"SQL error: {msg}"


def truncate_results(results: list, max_rows: int = 1000) -> tuple[list, bool]:
    """Truncate query results and return (truncated_results, was_truncated)."""
    if len(results) > max_rows:
        return results[:max_rows], True
    return results, False