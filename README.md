# Medical Database Query System

A natural language to SQL query system with synthetic medical data generation and interactive web interface.

## 🚀 Features

- **Natural Language Query Processing**
  - Ask questions in plain English
  - View SQL translations
  - Get formatted results
  - Real-time query execution

- **Synthetic Data Generation**
  - Realistic medical database schema
  - Faker-powered synthetic data
  - Configurable data volume
  - Automatic relationships

- **Interactive Web Interface**
  - Dark mode UI
  - Schema visualization
  - Query metrics
  - System logs

## 📋 Requirements

- Python 3.8+
- SQLite3
- Required Python packages:
```bash
pip install -r requirements.txt
```

Contents of requirements.txt:
```
streamlit>=1.28.0
pandas
faker
spacy
sqlite3
python-dateutil
```

Additional setup:
```bash
# Install spaCy English model
python -m spacy download en_core_web_sm
```

## 🚀 Quick Start

1. **Setup Environment**
```bash
# Create virtual environment
python -m venv venv

# Activate environment
# Windows
.\venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

2. **First Run and Database Generation**
```bash
streamlit run streamlit_app.py
```

When you first run the application:
- You'll see a "Generate Sample Database" button
- Click it to create the medical.db database
- This generates:
  - 50 patients with random data
  - 10 doctors with specialties
  - 100 appointments over 60 days
- The database (medical.db) will be used for all future queries
- You can regenerate the database anytime by clicking the button again

3. **Using the System**
- After database generation, you'll see:
  - Database schema in the sidebar
  - Query input section
  - SQL translation display
  - Results area

## 💾 Database Generation Details

The system generates a SQLite database (medical.db) with:

**Sample Data Volume:**
- 50 Patients
  - Random names, ages (18-90)
  - Gender (M/F)
  - Blood types (A+, A-, B+, B-, O+, O-, AB+, AB-)

- 10 Doctors
  - Specialties: Cardiology, Neurology, Pediatrics, Oncology, General Practice
  - Professional naming (Dr. prefix)

- 100 Appointments
  - Spread over 60 days
  - Random patient-doctor assignments
  - Realistic appointment reasons
  - Proper date formatting

**Data Generation Features:**
- Uses Faker library for realistic names
- Maintains referential integrity
- Creates proper foreign key relationships
- Generates recent appointments (last 60 days)
- Ensures even distribution of specialties

## 💾 Database Schema

The system uses a medical database with three main tables:

**Patients**
- patient_id (PRIMARY KEY)
- name
- age
- gender
- blood_type

**Doctors**
- doctor_id (PRIMARY KEY)
- name
- specialty

**Appointments**
- appointment_id (PRIMARY KEY)
- patient_id (FOREIGN KEY)
- doctor_id (FOREIGN KEY)
- date
- reason

## 📝 Sample Queries

```plaintext
# Patient Queries
Show all patients                           # Lists all patients
Show all patients older than 60             # Elderly patients
List patients with blood type A+            # Blood type filter

# Doctor Queries
List all doctors and their specialties      # Doctor directory
How many doctors are there?                 # Count doctors
Show doctors who have the most appointments # Busiest doctors

# Time-based Queries
Show appointments in the last month         # Monthly view
Show appointments in the last week          # Weekly view
Show appointments in the last 3 days        # Recent view
```

## 🔄 System Workflow

1. **Database Generation**
   - Click "Generate Sample Database" if no database exists
   - System creates tables and relationships
   - Faker generates realistic sample data
   - Database is populated with synthetic records

2. **Query Processing**
   - Enter natural language query
   - System translates to SQL
   - Query is validated for safety
   - Results are displayed with metrics

3. **Results Display**
   - Data shown in interactive table
   - Processing metrics displayed
   - SQL query visible for verification
   - System logs available in sidebar

## ⚙️ Design Decisions

1. **Database**
   - SQLite for portability and ease of use
   - Foreign key constraints for data integrity
   - Indexed fields for query performance

2. **Data Generation**
   - Faker library for realistic data
   - Random but sensible relationships
   - Configurable data volume
   - Time-based appointment generation

3. **Query Interface**
   - Rule-based NL to SQL conversion
   - Pattern matching for reliable translation
   - SQL injection prevention
   - Clear error handling

## 🛠️ Limitations

1. **Query Types**
   - SELECT queries only (for security)
   - Predefined query patterns
   - Basic aggregations supported
   - No complex joins or subqueries

2. **Data Generation**
   - Fixed medical domain
   - Limited to three main tables
   - Synthetic data only
   - Basic relationships

3. **Performance**
   - In-memory query processing
   - No query optimization
   - Limited to moderate data sizes
   - Single-user operation

## 🔄 Future Improvements

1. **Query Processing**
   - Add support for complex queries
   - Implement query optimization
   - Add more aggregation functions
   - Support multiple domains

2. **Data Generation**
   - Add more medical specialties
   - Support custom schemas
   - Add more realistic relationships
   - Include more medical data types

3. **Interface**
   - Add result visualizations
   - Implement query history
   - Add export functionality
   - Support batch operations

## 📄 License

MIT License

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request# Medical Natural Language to SQL Query System

A streamlined system for querying medical databases using natural language. Convert plain English questions into SQL queries for medical data analysis.

## Overview

This project provides a web interface for querying a medical database using natural language. It converts English questions into SQL queries and displays the results in an interactive dashboard.

## Features

- Natural language to SQL conversion
- Interactive Streamlit dashboard
- Real-time query processing
- Medical data visualization
- Safe query execution (SELECT-only)
- Synthetic medical database for testing

## Quick Start

1. Create a virtual environment and activate it:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

3. Generate sample database:
```powershell
python src/data_generation/medical_db.py --patients 200 --doctors 20 --appointments 600
```

4. Run the application:
```powershell
streamlit run streamlit_app.py
```

## Project Structure

```
medical_database_system/
├── data/               # Database files and schema
├── examples/           # Sample queries
├── src/               # Source code
│   ├── database/      # Database operations
│   ├── nl_to_sql/    # Query translation
│   └── data_generation/ # Data generation
├── tests/             # Unit tests
└── streamlit_app.py   # Main application
```

## Notes
- The Transformers model may be large and slow locally; if you don't have a suitable model installed, the app uses a conservative rule-based fallback for common queries.
- The app sanitizes generated SQL and only executes SELECT queries. It blocks queries that include keywords like `INSERT`, `UPDATE`, `DROP`, etc. Use only trusted models.
- This demo is for educational purposes and uses synthetic data generated by Faker.

Extending to other domains
- Update `schema.py` with a new schema and adapt the data generation functions in `generate_data.py`.
- Optionally fine-tune or use a different local text-to-SQL model in `text2sql_model.py`.

## Sample Queries

- "Show all patients older than 60"
- "How many appointments did Dr. Lee have in 2023?"
- "List medications prescribed to John Doe"

## Testing

```powershell
pytest -q
```

License: MIT
