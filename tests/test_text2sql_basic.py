"""Test the rule-based text-to-SQL translator (fallback without model)."""
import pytest
from text2sql_model import Text2SQLModel


@pytest.fixture
def translator():
    """Create a Text2SQLModel instance with test schema."""
    schema = {
        "patients": ["patient_id", "name", "age", "gender"],
        "doctors": ["doctor_id", "name", "specialty"],
        "medications": ["med_id", "name", "manufacturer"],
    }
    return Text2SQLModel(schema=schema)


def test_basic_patient_age_query(translator):
    """Test basic 'patients older than X' pattern."""
    nl_query = "Show me all patients older than 60"
    sql = translator.nl_to_sql(nl_query)
    assert "SELECT" in sql.upper()
    assert "FROM patients" in sql
    assert "age > 60" in sql


def test_count_appointments_query(translator):
    """Test 'count appointments for doctor' pattern."""
    nl_query = "How many appointments did Dr. Smith have in 2023"
    sql = translator.nl_to_sql(nl_query)
    assert "SELECT COUNT" in sql.upper()
    assert "appointments" in sql.lower()
    assert "2023" in sql


def test_medications_for_patient_query(translator):
    """Test 'medications for patient' pattern with JOINs."""
    nl_query = "List medications prescribed to patient John Doe"
    sql = translator.nl_to_sql(nl_query)
    assert "SELECT" in sql.upper()
    assert "JOIN" in sql.upper()
    assert "medications" in sql
    assert "John Doe" in sql


def test_list_all_query(translator):
    """Test simple 'list all X' pattern."""
    nl_query = "List all patients"
    sql = translator.nl_to_sql(nl_query)
    assert sql == "SELECT * FROM patients LIMIT 200"


def test_unknown_query_pattern(translator):
    """Test fallback for unknown question patterns."""
    nl_query = "What is the meaning of life?"
    sql = translator.nl_to_sql(nl_query)
    assert sql.startswith("--")  # Returns explanatory comment