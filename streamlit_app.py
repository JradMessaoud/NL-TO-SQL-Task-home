import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
import random
from faker import Faker
from src.utils.logging_config import setup_logging

# Initialize logging
setup_logging()
logger = logging.getLogger("QuerySystem.UI")

# Initialize Faker
fake = Faker()

def generate_sample_database(db_path="medical.db"):
    """Generate a sample medical database."""
    with sqlite3.connect(db_path) as conn:
        # Create tables
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS patients (
                patient_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                gender TEXT NOT NULL,
                blood_type TEXT
            );
            
            CREATE TABLE IF NOT EXISTS doctors (
                doctor_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                specialty TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS appointments (
                appointment_id INTEGER PRIMARY KEY,
                patient_id INTEGER,
                doctor_id INTEGER,
                date TEXT NOT NULL,
                reason TEXT,
                FOREIGN KEY (patient_id) REFERENCES patients (patient_id),
                FOREIGN KEY (doctor_id) REFERENCES doctors (doctor_id)
            );
        """)
        
        # Generate sample data
        # Patients
        patients = [(i, fake.name(), random.randint(18, 90), 
                    random.choice(['M', 'F']),
                    random.choice(['A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-']))
                   for i in range(1, 51)]
        
        # Doctors
        specialties = ['Cardiology', 'Neurology', 'Pediatrics', 'Oncology', 'General Practice']
        doctors = [(i, f"Dr. {fake.name()}", random.choice(specialties))
                  for i in range(1, 11)]
        
        # Appointments
        start_date = datetime.now() - timedelta(days=30)
        appointments = []
        for i in range(1, 101):
            date = start_date + timedelta(days=random.randint(0, 60))
            appointments.append((
                i,
                random.randint(1, 50),  # patient_id
                random.randint(1, 10),   # doctor_id
                date.strftime('%Y-%m-%d'),
                fake.sentence(nb_words=4)
            ))
        
        # Insert data
        conn.executemany("INSERT OR REPLACE INTO patients VALUES (?,?,?,?,?)", patients)
        conn.executemany("INSERT OR REPLACE INTO doctors VALUES (?,?,?)", doctors)
        conn.executemany("INSERT OR REPLACE INTO appointments VALUES (?,?,?,?,?)", appointments)
        
        conn.commit()
    
    return db_path

# Set page config
st.set_page_config(
    page_title="Medical Database Query System",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database setup and functions
def get_db_path():
    """Get the path to the SQLite database."""
    # Look for .db files in the current directory
    db_files = list(Path('.').glob('*.db'))
    if db_files:
        return str(db_files[0])
    return None

def execute_query(query):
    """Execute an SQL query and return the results as a DataFrame."""
    if not DB_PATH:
        logger.error("Database not found")
        raise Exception("Database not found. Please generate a database first.")
    
    try:
        logger.info(f"Executing query: {query}")
        start_time = datetime.now()
        with sqlite3.connect(DB_PATH) as conn:
            results = pd.read_sql_query(query, conn)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"Query executed successfully in {execution_time:.2f} seconds. Returned {len(results)} rows.")
        return results
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise

def is_safe_query(query):
    """Basic SQL query safety check."""
    query = query.lower()
    if any(word in query for word in ['insert', 'update', 'delete', 'drop', 'alter', 'create']):
        return False
    return True

DB_PATH = get_db_path()
if not DB_PATH:
    st.error("No database file found. Please generate a database first.")

# Custom CSS
st.markdown("""
<style>
.main > div {
    padding: 2em;
    background-color: #1e1e1e;
}
.stTextInput > div > div > input {
    padding: 15px;
    background-color: #2d2d2d;
    color: #ffffff;
    border: 1px solid #4a4a4a;
}
.stTextArea > div > div > textarea {
    padding: 15px;
    background-color: #2d2d2d;
    color: #ffffff;
    border: 1px solid #4a4a4a;
}
div[data-testid="stExpander"] div[role="button"] p {
    font-size: 1.1em;
    color: #00cf86;
}
.stAlert > div {
    padding: 1em;
    border-radius: 8px;
    background-color: #2d2d2d;
}
div[data-testid="stDataFrameContainer"] > div {
    max-height: 400px;
    background-color: #2d2d2d;
    border: 1px solid #4a4a4a;
}
.query-category {
    padding: 8px 15px;
    border-radius: 20px;
    background-color: #3d3d3d;
    color: #00cf86;
    margin: 4px;
    display: inline-block;
    font-weight: 500;
    border: 1px solid #00cf86;
}
h1, h2, h3, h4 {
    color: #00cf86 !important;
}
.stButton > button {
    background-color: #00cf86;
    color: #1e1e1e;
    font-weight: 600;
    border: none;
    padding: 10px 20px;
    border-radius: 8px;
}
.stButton > button:hover {
    background-color: #00b377;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background-color: #2d2d2d;
    padding: 10px;
    border-radius: 10px;
}
.stTabs [data-baseweb="tab"] {
    background-color: #3d3d3d;
    color: #ffffff;
    border-radius: 6px;
    padding: 8px 16px;
}
.stTabs [aria-selected="true"] {
    background-color: #00cf86;
    color: #1e1e1e;
}
</style>
""", unsafe_allow_html=True)

def get_sqlite_schema():
    """Get schema directly from SQLite database."""
    if not DB_PATH:
        return {}
    
    schema = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                # Store column info: name, type, notnull, dflt_value, pk
                schema[table_name] = {
                    col[1]: {
                        "type": col[2],
                        "nullable": not col[3],
                        "primary_key": bool(col[5])
                    }
                    for col in columns
                }
    except Exception as e:
        st.sidebar.error(f"Error reading schema: {str(e)}")
        return {}
    
    return schema

def show_schema():
    """Display the database schema in the sidebar."""
    schema = get_sqlite_schema()
    
    st.sidebar.title("Database Schema")
    if not schema:
        st.sidebar.warning("No schema available. Generate a database first.")
        return
        
    for table, columns in schema.items():
        with st.sidebar.expander(f"📋 {table}"):
            for col_name, col_info in columns.items():
                pk_mark = "🔑 " if col_info["primary_key"] else ""
                nullable = "nullable" if col_info["nullable"] else "not null"
                st.write(f"{pk_mark}{col_name}: {col_info['type']} ({nullable})")

def main():
    # Title and Introduction
    st.title("🏥 Medical Database Query System")
    
    # Add logging display in sidebar
    with st.sidebar.expander("📊 System Logs", expanded=False):
        try:
            with open("logs/query_system.log", "r") as f:
                logs = f.readlines()[-10:]  # Show last 10 log entries
                for log in logs:
                    if "ERROR" in log:
                        st.error(log.strip())
                    elif "WARNING" in log:
                        st.warning(log.strip())
                    else:
                        st.text(log.strip())
        except Exception as e:
            st.warning(f"Could not load logs: {str(e)}")
    
    # Database Generation Section
    if not DB_PATH:
        st.warning("No database found. Generate a sample database to get started.")
        if st.button("Generate Sample Database"):
            try:
                db_path = generate_sample_database()
                st.success(f"Sample database generated successfully at {db_path}")
                st.info("Please restart the application to use the new database.")
                st.stop()
            except Exception as e:
                st.error(f"Error generating database: {str(e)}")
                st.stop()
        st.stop()
    
    # Show database schema in sidebar
    show_schema()
    
    # Step 1: Input
    st.header("1. Natural Language Query")
    st.write("Ask a question about the medical database in plain English.")
    
    # Initialize session state for user question if not exists
    if 'user_question' not in st.session_state:
        st.session_state.user_question = ""
    
    # Text input for query with session state value
    question = st.text_area(
        "Enter your question in natural language:",
        value=st.session_state.user_question,
        height=100,
        key="unique_query_input",
        help="Type your question in plain English. You can find example queries in 'examples/example_queries.txt'"
    )
    
    if st.button("Run Query", type="primary"):
        if question:
            logger.info(f"Processing new query: {question}")
            start_time = datetime.now()
            
            # Create metrics columns
            col1, col2, col3 = st.columns(3)
            
            # Step 2: Translation
            st.header("2. SQL Translation")
            with st.spinner("Translating to SQL..."):
                try:
                    # Create Text2SQL converter
                    from src.nl_to_sql.text2sql_model import Text2SQLModel
                    converter = Text2SQLModel()
                    sql = converter.convert(question)
                    
                    # Calculate translation time
                    translation_time = (datetime.now() - start_time).total_seconds()
                    with col1:
                        st.metric("Translation Time", f"{translation_time:.2f}s")
                    
                    if sql:
                        st.code(sql, language="sql")
                        
                        # Step 3: Execution
                        st.header("3. Query Results")
                        if is_safe_query(sql):
                            execution_start = datetime.now()
                            results = execute_query(sql)
                            
                            # Calculate execution time
                            execution_time = (datetime.now() - execution_start).total_seconds()
                            with col2:
                                st.metric("Execution Time", f"{execution_time:.2f}s")
                            
                            if not results.empty:
                                with col3:
                                    st.metric("Results Found", len(results))
                                st.dataframe(results)
                            else:
                                st.info("Query executed successfully but returned no results.")
                                with col3:
                                    st.metric("Results Found", 0)
                        else:
                            st.error("Only SELECT queries are allowed for security reasons.")
                    else:
                        st.warning("Could not understand the question. Try rephrasing it or check the example queries.")
                except Exception as e:
                    logger.error(f"Query processing error: {str(e)}")
                    st.error(f"Error: {str(e)}")
                finally:
                    # Calculate total processing time
                    total_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Total query processing time: {total_time:.2f} seconds")
        else:
            logger.warning("Empty query submitted")
            st.warning("Please enter a question first.")
    
    # Additional Information
    with st.expander("ℹ️ How to use"):
        st.write("""
        1. Type your question in natural language above
        2. The system will translate it to SQL
        3. The query will run against the database
        4. Results will be displayed in a table
        
        View the schema in the sidebar to see available tables and fields.
        Try the example queries above for guidance on how to phrase your questions.
        """)

if __name__ == "__main__":
    main()