# Database configuration
DB_PATH = "medical.db"
LOG_DIR = "logs"
LOG_FILE = "app.log"

# Model configuration
DEFAULT_MODEL = None  # Set to a local model name to use, e.g. "tscholak/text-to-sql-t5-large"
MODEL_MAX_LENGTH = 512

# Data generation defaults
DEFAULT_COUNTS = {
    "patients": 200,
    "doctors": 20,
    "appointments": 600,
    "medications": 40,
    "prescriptions": 400
}

# Query constraints
MAX_ROWS_RETURN = 1000
QUERY_TIMEOUT_SEC = 30

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"

# Schema configuration (matches schema.json)
SCHEMA = {
    "patients": ["patient_id", "name", "age", "gender"],
    "doctors": ["doctor_id", "name", "specialty"],
    "medications": ["med_id", "name", "manufacturer"],
    "appointments": ["appt_id", "patient_id", "doctor_id", "date", "reason"],
    "prescriptions": ["presc_id", "patient_id", "med_id", "dosage", "date"]
}