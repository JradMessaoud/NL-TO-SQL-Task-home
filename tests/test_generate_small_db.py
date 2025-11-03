import os
import sqlite3
from generate_data import generate


def test_generate_and_counts(tmp_path):
    dbf = tmp_path / "test_med.db"
    generate(db_path=str(dbf), patients=10, doctors=3, appointments=30, medications=5, prescriptions=20)
    assert os.path.exists(dbf)
    conn = sqlite3.connect(str(dbf))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM patients")
    pts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM doctors")
    docs = cur.fetchone()[0]
    assert pts == 10
    assert docs == 3
    conn.close()
