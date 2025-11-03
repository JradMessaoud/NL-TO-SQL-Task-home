"""End-to-end tests using a small test database."""
import os
import pytest
import sqlite3
from generate_data import generate
from text2sql_model import Text2SQLModel
from db import execute_select


@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    """Create a small test database for all tests."""
    tmp_dir = tmp_path_factory.mktemp("data")
    db_path = str(tmp_dir / "test_medical.db")
    
    # Generate a small dataset
    generate(
        db_path=db_path,
        patients=10,
        doctors=3,
        appointments=20,
        medications=5,
        prescriptions=15
    )
    
    return db_path


@pytest.fixture
def model():
    """Create Text2SQLModel with schema."""
    from config import SCHEMA
    return Text2SQLModel(schema=SCHEMA)


def test_patient_age_query(test_db, model):
    """Test complete flow: NL -> SQL -> results for age query."""
    question = "Show me all patients older than 50"
    sql = model.nl_to_sql(question)
    
    assert sql and "SELECT" in sql.upper()
    cols, rows = execute_select(test_db, sql)
    
    assert "age" in cols
    assert all(row[cols.index("age")] > 50 for row in rows)


def test_appointment_count_query(test_db, model):
    """Test complete flow: NL -> SQL -> results for appointment counts."""
    # First find a doctor in the test DB
    cols, rows = execute_select(test_db, "SELECT name FROM doctors LIMIT 1")
    doctor_name = rows[0][0]
    
    question = f"How many appointments did {doctor_name} have in 2023"
    sql = model.nl_to_sql(question)
    
    assert sql and "COUNT" in sql.upper()
    cols, rows = execute_select(test_db, sql)
    assert len(rows) == 1
    assert isinstance(rows[0][0], int)  # Count returns integer


def test_prescription_query(test_db, model):
    """Test complete flow: NL -> SQL -> results for prescription query."""
    # First find a patient in the test DB
    cols, rows = execute_select(test_db, "SELECT name FROM patients LIMIT 1")
    patient_name = rows[0][0]
    
    question = f"List all medications prescribed to patient {patient_name}"
    sql = model.nl_to_sql(question)
    
    assert sql and "JOIN" in sql.upper()
    cols, rows = execute_select(test_db, sql)
    assert "name" in cols  # Medication names


def test_invalid_query_safe_failure(test_db, model):
    """Test that invalid questions fail safely."""
    question = "DROP TABLE patients"  # Malicious
    sql = model.nl_to_sql(question)
    
    # Should get comment back
    assert sql.startswith("--")
    
    # Attempt to execute should fail safely
    with pytest.raises(ValueError):
        execute_select(test_db, sql)