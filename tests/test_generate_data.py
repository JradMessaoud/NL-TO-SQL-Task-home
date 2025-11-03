"""Test the data generation module with various sizes and constraints."""
import os
import sqlite3
import pytest
from generate_data import generate
from config import DEFAULT_COUNTS


def test_generate_default_counts(tmp_path):
    """Test generation with default record counts."""
    dbf = tmp_path / "test_default.db"
    generate(db_path=str(dbf))
    
    conn = sqlite3.connect(str(dbf))
    cur = conn.cursor()
    
    # Check counts match defaults
    for table, count in DEFAULT_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        assert cur.fetchone()[0] == count
    
    conn.close()


def test_generate_small_counts(tmp_path):
    """Test generation with small counts to verify FK relationships."""
    dbf = tmp_path / "test_small.db"
    small_counts = {
        "patients": 5,
        "doctors": 2,
        "appointments": 10,
        "medications": 3,
        "prescriptions": 8
    }
    
    generate(db_path=str(dbf), **small_counts)
    conn = sqlite3.connect(str(dbf))
    cur = conn.cursor()
    
    # Verify FK integrity
    cur.execute("""
        SELECT COUNT(*) FROM appointments a
        LEFT JOIN patients p ON a.patient_id = p.patient_id
        LEFT JOIN doctors d ON a.doctor_id = d.doctor_id
        WHERE p.patient_id IS NULL OR d.doctor_id IS NULL
    """)
    assert cur.fetchone()[0] == 0  # No orphaned FKs
    
    cur.execute("""
        SELECT COUNT(*) FROM prescriptions p
        LEFT JOIN patients pa ON p.patient_id = pa.patient_id
        LEFT JOIN medications m ON p.med_id = m.med_id
        WHERE pa.patient_id IS NULL OR m.med_id IS NULL
    """)
    assert cur.fetchone()[0] == 0  # No orphaned FKs
    
    conn.close()


def test_generate_unique_ids(tmp_path):
    """Test that generated IDs are unique within tables."""
    dbf = tmp_path / "test_unique.db"
    generate(db_path=str(dbf), patients=100, doctors=10)
    
    conn = sqlite3.connect(str(dbf))
    cur = conn.cursor()
    
    # Check for duplicate IDs
    for table in ["patients", "doctors"]:
        cur.execute(f"""
            SELECT {table[:-1]}_id, COUNT(*)
            FROM {table}
            GROUP BY {table[:-1]}_id
            HAVING COUNT(*) > 1
        """)
        assert not cur.fetchall()  # No duplicates
    
    conn.close()


def test_generate_realistic_dates(tmp_path):
    """Test that generated dates are in reasonable ranges."""
    dbf = tmp_path / "test_dates.db"
    generate(db_path=str(dbf))
    
    conn = sqlite3.connect(str(dbf))
    cur = conn.cursor()
    
    # Check appointment dates (should be within last 3 years)
    cur.execute("SELECT date FROM appointments")
    dates = [row[0] for row in cur.fetchall()]
    assert all(d >= "2022" for d in dates)  # No dates before 2022
    
    conn.close()