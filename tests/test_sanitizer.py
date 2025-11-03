from db import is_safe_select


def test_allow_simple_select():
    ok, norm = is_safe_select("SELECT name FROM patients")
    assert ok
    assert norm.upper().startswith("SELECT")


def test_disallow_drop():
    ok, _ = is_safe_select("DROP TABLE patients;")
    assert not ok


def test_disallow_multiple_statements():
    ok, _ = is_safe_select("SELECT * FROM patients; DELETE FROM patients;")
    assert not ok
