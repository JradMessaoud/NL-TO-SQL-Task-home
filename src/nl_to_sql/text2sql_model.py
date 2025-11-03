"""
Rule-based natural language to SQL converter for medical database queries.
Uses explicit rules and patterns for robust query understanding.
"""
import re
import spacy
import sqlparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

# Set up logger
logger = logging.getLogger("QuerySystem.Text2SQL")

# Load spaCy model
try:
    logger.info("Loading spaCy model...")
    nlp = spacy.load("en_core_web_sm")
    logger.info("spaCy model loaded successfully")
except:
    logger.warning("spaCy model not found, downloading...")
    spacy.cli.download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")
    logger.info("spaCy model downloaded and loaded successfully")


class QueryRule:
    """Represents a single query matching rule."""
    
    def __init__(self, name: str, patterns: List[str], sql_template: str, 
                 extract_params: Optional[callable] = None,
                 validate_params: Optional[callable] = None):
        """Initialize a query rule.
        
        Args:
            name: Rule identifier
            patterns: List of regex patterns to match
            sql_template: SQL query template with {param} placeholders
            extract_params: Function to extract parameters from match
            validate_params: Function to validate extracted parameters
        """
        self.name = name
        self.patterns = [re.compile(p, re.IGNORECASE) for p in patterns]
        self.sql_template = sql_template
        self.extract_params = extract_params or (lambda x: x.groups())
        self.validate_params = validate_params or (lambda x: True)


class Text2SQLModel:
    """Convert natural language medical queries to SQL using NLP and pattern matching."""
    
    def __init__(self):
        """Initialize the converter with query patterns and NLP components."""
        # Query type patterns with improved matching
        self.query_types = {
            "count": r"(?:how many|count|number of|total number of)",
            "show": r"(?:show|list|display|get|find|what are|who are|show me)",
            "average": r"(?:average|mean|avg)",
            "threshold": r"(?:more than|greater than|at least|over)\s+(\d+)",
            "analytics": r"(?:most|busiest|highest|more than|greater than|who have)",
            "comparison": r"(?:more than|greater than|less than|fewer than|at least|over|older than)"
        }
        
        # Common table names and their variations
        self.term_map = {
            "patient": ["patient", "patients", "person", "people"],
            "doctor": ["doctor", "doctors", "physician", "physicians", "specialist", "specialists", "dr", "dr."],
            "appointment": ["appointment", "appointments", "visit", "visits", "consultation", "consultations"],
            "specialty": ["specialty", "specialties", "specialization", "specializations"]
        }

        # Entity patterns
        self.patterns = {
            # Enhanced analytics queries
            "doctors_most_appointments": {
                "pattern": r"(?:show|list|get|find|who are).*(?:the\s+)?doctors?.*(?:most|busiest|highest number of|with the most).*appointments?",
                "sql": lambda _: """
                    WITH DoctorStats AS (
                        SELECT 
                            d.doctor_id,
                            d.name,
                            d.specialty,
                            COUNT(DISTINCT a.appointment_id) as appointment_count,
                            COUNT(DISTINCT a.patient_id) as unique_patients,
                            COUNT(DISTINCT DATE(a.date)) as days_with_appointments,
                            MAX(a.date) as last_appointment,
                            ROUND(COUNT(a.appointment_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM appointments), 0), 2) as percentage
                        FROM doctors d
                        LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                        GROUP BY d.doctor_id, d.name, d.specialty
                        HAVING appointment_count > 0
                    )
                    SELECT 
                        name,
                        specialty,
                        appointment_count,
                        unique_patients,
                        days_with_appointments,
                        ROUND(appointment_count * 1.0 / NULLIF(days_with_appointments, 0), 2) as avg_appointments_per_day,
                        last_appointment,
                        percentage || '%' as workload_percentage
                    FROM DoctorStats
                    ORDER BY appointment_count DESC
                    LIMIT 5"""
            },
            "doctors_with_min_appointments": {
                "pattern": r"(?:show|list|get|find)?.*doctors?.*(?:with|having|" + self.query_types["threshold"] + ").*appointments?",
                "sql": lambda threshold: f"""
                    SELECT 
                        d.name, 
                        d.specialty, 
                        COUNT(a.appointment_id) as appointment_count,
                        ROUND(COUNT(a.appointment_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM appointments), 0), 2) as percentage
                    FROM doctors d
                    LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                    GROUP BY d.doctor_id, d.name, d.specialty
                    HAVING COUNT(a.appointment_id) > {threshold}
                    ORDER BY appointment_count DESC"""
            },
            # Patient queries matching example patterns exactly
            "all_patients": {
                "pattern": r"^show all patients?$",
                "sql": lambda _: """
                    SELECT 
                        name,
                        age,
                        gender,
                        blood_type,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) as appointment_count,
                        (SELECT MAX(date) FROM appointments a WHERE a.patient_id = p.patient_id) as last_visit
                    FROM patients p
                    ORDER BY name"""
            },
            "patients_by_age": {
                "pattern": r"^show all patients? older than (\d+)$",
                "sql": lambda age: f"""
                    SELECT 
                        name,
                        age,
                        gender,
                        blood_type,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) as appointment_count
                    FROM patients p
                    WHERE age > {int(age)}
                    ORDER BY age DESC, name"""
            },
            "patients_by_blood": {
                "pattern": r"^(?:list|show) patients? with blood type ([ABO][+-])$",
                "sql": lambda blood_type: f"""
                    SELECT 
                        name,
                        age,
                        gender,
                        blood_type,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) as appointment_count,
                        (SELECT MAX(date) FROM appointments a WHERE a.patient_id = p.patient_id) as last_visit
                    FROM patients p
                    WHERE blood_type = '{blood_type.upper()}'
                    ORDER BY name"""
            },
            "patient_age": {
                "pattern": r"(?:show|list|get)?\s*(?:all\s+)?(?:patients?|people).*(?:over|older than)\s+(\d+)(?:\s+years)?.*(?:old)?",
                "sql": lambda age: f"""
                    SELECT 
                        name,
                        age,
                        gender,
                        blood_type,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) as appointment_count
                    FROM patients p 
                    WHERE age > {int(age)}
                    ORDER BY age DESC, name"""
            },
            "patient_blood_type": {
                "pattern": r"(?:list|show|get|find)?\s*(?:all\s+)?(?:patients?|people).*(?:with|having|of|type|blood type)\s+([ABO][+-])",
                "sql": lambda blood_type: f"""
                    SELECT 
                        name,
                        age,
                        gender,
                        blood_type,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) as appointment_count,
                        (SELECT MAX(date) FROM appointments a WHERE a.patient_id = p.patient_id) as last_visit
                    FROM patients p
                    WHERE blood_type = '{blood_type.upper()}'
                    ORDER BY 
                        CASE WHEN (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) > 0 THEN 0 ELSE 1 END,
                        (SELECT COUNT(*) FROM appointments a WHERE a.patient_id = p.patient_id) DESC,
                        name"""
            },
            "count_patients": {
                "pattern": r"(?:how many|count|number of|total).*(?:patients?|people)(?:\s+do\s+we\s+have)?",
                "sql": lambda _: """
                    SELECT COUNT(*) as total_patients,
                           SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) as male_patients,
                           SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) as female_patients
                    FROM patients"""
            },
            
            # Doctor queries matching example patterns
            "all_doctors": {
                "pattern": r"^list all doctors? and their specialties$",
                "sql": lambda _: """
                    SELECT 
                        d.name,
                        d.specialty,
                        COUNT(DISTINCT a.appointment_id) as appointment_count,
                        COUNT(DISTINCT p.patient_id) as unique_patients
                    FROM doctors d
                    LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                    LEFT JOIN patients p ON a.patient_id = p.patient_id
                    GROUP BY d.doctor_id, d.name, d.specialty
                    ORDER BY d.specialty, d.name"""
            },
            "doctors_by_appointments": {
                "pattern": r"^show doctors?(?: who have)? (?:with |having )?(?:the )?(?:most|more than \d+) appointments?$",
                "sql": lambda threshold=None: f"""
                    WITH DoctorStats AS (
                        SELECT 
                            d.doctor_id,
                            d.name,
                            d.specialty,
                            COUNT(DISTINCT a.appointment_id) as appointment_count,
                            COUNT(DISTINCT p.patient_id) as unique_patients,
                            COUNT(DISTINCT DATE(a.date)) as active_days
                        FROM doctors d
                        LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                        LEFT JOIN patients p ON a.patient_id = p.patient_id
                        GROUP BY d.doctor_id, d.name, d.specialty
                        {'HAVING appointment_count > ' + str(threshold) if threshold else ''}
                    )
                    SELECT 
                        name,
                        specialty,
                        appointment_count,
                        unique_patients,
                        ROUND(appointment_count * 1.0 / NULLIF(active_days, 0), 2) as avg_daily_appointments
                    FROM DoctorStats
                    ORDER BY appointment_count DESC
                    {'' if threshold else 'LIMIT 5'}"""
            },
            "count_doctors": {
                "pattern": r"(?:how many|count|number of)\s+doctors?\s+(?:are\s+there|do\s+we\s+have|in\s+total)?(?!\s+(?:in\s+each|by|per)\s+specialty)",
                "sql": lambda _: """
                    SELECT 
                        COUNT(*) as total_doctors,
                        COUNT(DISTINCT specialty) as unique_specialties,
                        SUM(
                            (SELECT COUNT(*) FROM appointments a WHERE a.doctor_id = d.doctor_id)
                        ) as total_appointments,
                        ROUND(AVG(
                            (SELECT COUNT(*) FROM appointments a WHERE a.doctor_id = d.doctor_id)
                        ), 2) as avg_appointments_per_doctor
                    FROM doctors d"""
            },
            "count_doctors_by_specialty": {
                "pattern": r"(?:how many|count|number of)\s+doctors?\s+(?:are\s+there\s+)?(?:in\s+each|by|per|across|for\s+each)\s+specialty",
                "sql": lambda _: """
                    WITH SpecialtyStats AS (
                        SELECT 
                            d.specialty,
                            COUNT(DISTINCT d.doctor_id) as doctor_count,
                            COUNT(DISTINCT a.appointment_id) as total_appointments,
                            COUNT(DISTINCT a.patient_id) as total_patients,
                            COUNT(DISTINCT DATE(a.date)) as active_days
                        FROM doctors d
                        LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                        GROUP BY d.specialty
                    )
                    SELECT 
                        specialty,
                        doctor_count,
                        total_appointments,
                        total_patients,
                        ROUND(total_appointments * 1.0 / NULLIF(doctor_count, 0), 2) as avg_appointments_per_doctor,
                        ROUND(total_patients * 1.0 / NULLIF(doctor_count, 0), 2) as avg_patients_per_doctor,
                        ROUND(total_appointments * 100.0 / NULLIF((SELECT COUNT(*) FROM appointments), 0), 2) || '%' as workload_percentage
                    FROM SpecialtyStats
                    ORDER BY doctor_count DESC"""
            },
            
            # Time-based appointment queries with improved patterns
            "recent_appointments": {
                "pattern": r"(?:show|list|display|get|what are|find)?\s*appointments?\s*(?:in|from|for|during|within|over)?\s*(?:the\s+)?(?:last|past|recent|previous)?\s*(\d+|a|the|this)?\s*(day|days|week|weeks|month|months|year|years?)(?:\s+ago)?",
                "sql": lambda number, unit: f"""
                    WITH date_ranges AS (
                        SELECT 
                            date(MAX(date)) as last_date,
                            CASE 
                                WHEN '{unit.lower().rstrip('s')}' = 'month' THEN
                                    date(MAX(date), 'start of month')
                                WHEN '{unit.lower().rstrip('s')}' = 'week' THEN
                                    date(MAX(date), 'weekday 0', '-0 days')
                                ELSE
                                    date(MAX(date), '+1 day')
                            END as period_end,
                            CASE 
                                WHEN '{unit.lower().rstrip('s')}' = 'month' THEN
                                    date(MAX(date), 'start of month', '-1 month')
                                WHEN '{unit.lower().rstrip('s')}' = 'week' THEN
                                    date(MAX(date), 'weekday 0', '-7 days')
                                ELSE
                                    date(MAX(date), '-{number} days')
                            END as period_start
                        FROM appointments
                    ),
                    appointment_stats AS (
                        SELECT 
                            COUNT(DISTINCT a.doctor_id) as total_doctors,
                            COUNT(DISTINCT a.patient_id) as total_patients,
                            COUNT(*) as total_appointments,
                            ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(a.date)), 2) as avg_daily_appointments
                        FROM appointments a, date_ranges d
                        WHERE date(a.date) >= d.period_start
                        AND date(a.date) < d.period_end
                    )
                    SELECT 
                        a.date,
                        p.name as patient,
                        p.age,
                        p.blood_type,
                        d.name as doctor,
                        d.specialty,
                        a.reason,
                        stats.total_doctors,
                        stats.total_patients,
                        stats.total_appointments,
                        stats.avg_daily_appointments,
                        dr.period_start as period_start,
                        dr.period_end as period_end
                    FROM appointments a
                    JOIN patients p ON a.patient_id = p.patient_id
                    JOIN doctors d ON a.doctor_id = d.doctor_id
                    CROSS JOIN appointment_stats stats
                    CROSS JOIN date_ranges dr
                    WHERE date(a.date) >= dr.period_start
                    AND date(a.date) < dr.period_end
                    ORDER BY a.date DESC"""
            },
            "count_recent_appointments": {
                "pattern": r"(?:how many|count|number of)\s+appointments?\s*(?:in|from|for|during|within|over)\s*(?:the\s+)?(?:last|past|previous)\s*(\d+|a|the|this)?\s*(day|days|week|weeks|month|months|year|years?)(?:\s+ago)?",
                "sql": lambda number, unit: f"""
                    WITH TimeBasedStats AS (
                        SELECT 
                            COUNT(*) as total_appointments,
                            COUNT(DISTINCT patient_id) as unique_patients,
                            COUNT(DISTINCT doctor_id) as doctors_involved,
                            COUNT(DISTINCT DATE(date)) as unique_days,
                            MIN(date) as period_start,
                            MAX(date) as period_end
                        FROM appointments
                        WHERE date(date) >= {self._get_date_filter(number, unit)}
                    )
                    SELECT 
                        total_appointments,
                        unique_patients,
                        doctors_involved,
                        unique_days,
                        ROUND(total_appointments * 1.0 / NULLIF(unique_days, 0), 2) as avg_appointments_per_day,
                        period_start as from_date,
                        period_end as to_date
                    FROM TimeBasedStats"""
            },
            "doctor_appointments": {
                "pattern": r"(?:show|list|get|what are)?.*appointments?.*(?:for |with |by |of )?(?:doctor|dr\.?)\s+([A-Za-z\s]+)",
                "sql": lambda name: f"""
                    SELECT 
                        a.date,
                        p.name as patient,
                        p.age,
                        p.blood_type,
                        a.reason,
                        (
                            SELECT COUNT(*)
                            FROM appointments a2
                            WHERE a2.patient_id = p.patient_id
                            AND a2.doctor_id = d.doctor_id
                            AND a2.date < a.date
                        ) as previous_visits
                    FROM appointments a
                    JOIN doctors d ON a.doctor_id = d.doctor_id
                    JOIN patients p ON a.patient_id = p.patient_id
                    WHERE d.name LIKE '%{name.strip()}%'
                    ORDER BY a.date DESC"""
            },
            "patient_appointments": {
                "pattern": r"(?:show|list|get|what are)?.*appointments?.*(patient|of|for)\s+([A-Za-z\s]+)",
                "sql": lambda name: f"""
                    SELECT 
                        a.date,
                        d.name as doctor,
                        d.specialty,
                        a.reason,
                        (
                            SELECT COUNT(*)
                            FROM appointments a2
                            WHERE a2.patient_id = p.patient_id
                            AND a2.doctor_id = d.doctor_id
                            AND a2.date < a.date
                        ) as previous_visits_with_doctor,
                        (
                            SELECT COUNT(DISTINCT doctor_id)
                            FROM appointments a3
                            WHERE a3.patient_id = p.patient_id
                        ) as total_doctors_seen
                    FROM appointments a
                    JOIN doctors d ON a.doctor_id = d.doctor_id
                    JOIN patients p ON a.patient_id = p.patient_id
                    WHERE p.name LIKE '%{name.strip()}%'
                    ORDER BY a.date DESC"""
            },
            
            # Blood type queries
            "blood_type": {
                "pattern": r"blood.*(?:type|group)\s+([ABO]+[+-])",
                "sql": lambda blood_type: f"""
                    SELECT name, age, blood_type
                    FROM patients
                    WHERE blood_type = '{blood_type.upper()}'
                    ORDER BY name"""
            },
            
            # Count queries
            "count_by_specialty": {
                "pattern": r"(?:how many|count).*doctors?.*(?:in each|by|per) specialty",
                "sql": lambda _: """
                    SELECT specialty, COUNT(*) as count
                    FROM doctors
                    GROUP BY specialty
                    ORDER BY count DESC"""
            }
        }
        
            # Count queries precisely matching example patterns
        count_queries = {
            "patients": {
                "pattern": r"^how many patients do we have\?$",
                "sql": lambda _: """
                    SELECT 
                        COUNT(*) as total_patients,
                        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) as male_patients,
                        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) as female_patients,
                        COUNT(DISTINCT blood_type) as blood_type_count,
                        ROUND(AVG(age), 1) as avg_age
                    FROM patients"""
            },
            "doctors": {
                "pattern": r"^how many doctors are there\?$",
                "sql": lambda _: """
                    SELECT 
                        COUNT(*) as total_doctors,
                        COUNT(DISTINCT specialty) as unique_specialties,
                        ROUND(AVG((
                            SELECT COUNT(*) 
                            FROM appointments a 
                            WHERE a.doctor_id = d.doctor_id
                        )), 2) as avg_appointments_per_doctor
                    FROM doctors d"""
            },
            "appointments": {
                "pattern": r"^(?:how many appointments are there\?|count total number of appointments)$",
                "sql": lambda _: """
                    SELECT 
                        COUNT(*) as total_appointments,
                        COUNT(DISTINCT patient_id) as unique_patients,
                        COUNT(DISTINCT doctor_id) as unique_doctors,
                        COUNT(DISTINCT DATE(date)) as unique_days
                    FROM appointments"""
            },
            "specialty_count": {
                "pattern": r"^how many doctors are there in each specialty\?$",
                "sql": lambda _: """
                    SELECT 
                        specialty,
                        COUNT(*) as doctor_count,
                        COUNT(DISTINCT (
                            SELECT a.patient_id 
                            FROM appointments a 
                            WHERE a.doctor_id = d.doctor_id
                        )) as total_patients,
                        SUM((
                            SELECT COUNT(*) 
                            FROM appointments a 
                            WHERE a.doctor_id = d.doctor_id
                        )) as total_appointments
                    FROM doctors d
                    GROUP BY specialty
                    ORDER BY doctor_count DESC"""
            }
        }
        
        self.patterns.update({f"count_{entity}": pattern_info 
                            for entity, pattern_info in count_queries.items()})

    def _get_date_filter(self, number: str, unit: str) -> str:
        """Generate the date filter expression for time-based queries.
        
        Args:
            number: Number of time units or 'a'/'the'/'this' for single unit
            unit: Time unit (day/week/month/year or plural forms)
            
        Returns:
            SQLite date filter expression with explicit dates
        """
        from datetime import datetime, timedelta
        
        # Get current date
        current_date = datetime.now()
        
        # Handle text numbers
        if not number or isinstance(number, str) and number.lower() in ('a', 'the', 'this'):
            number = '1'
        num = int(number)
        
        # Normalize unit by removing plurals and converting to lowercase
        unit = unit.lower().rstrip('s')
        
        # Calculate the start date based on the unit
        if unit == 'month':
            # Simple month calculation (might not be exact for varying month lengths)
            days_to_subtract = num * 30
            start_date = current_date - timedelta(days=days_to_subtract)
        elif unit == 'week':
            start_date = current_date - timedelta(weeks=num)
        elif unit == 'year':
            start_date = current_date - timedelta(days=num*365)
        else:  # days
            start_date = current_date - timedelta(days=num)
            
        # Format dates in SQLite format (YYYY-MM-DD)
        return f"'{start_date.strftime('%Y-%m-%d')}'"
            
    def _generate_time_query(self, number: str, unit: str) -> str:
        """Generate a query with time-based filtering and analytics."""
        date_filter = self._get_date_filter(number, unit)
        
        return f"""
            WITH appointment_stats AS (
                SELECT 
                    COUNT(DISTINCT a.doctor_id) as total_doctors,
                    COUNT(DISTINCT a.patient_id) as total_patients,
                    COUNT(*) as total_appointments,
                    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(a.date)), 2) as avg_daily_appointments
                FROM appointments a
                WHERE date(a.date) >= {date_filter}
            )
            SELECT 
                a.date,
                p.name as patient,
                p.age,
                p.blood_type,
                d.name as doctor,
                d.specialty,
                a.reason,
                stats.total_doctors,
                stats.total_patients,
                stats.total_appointments,
                stats.avg_daily_appointments
            FROM appointments a
            JOIN patients p ON a.patient_id = p.patient_id
            JOIN doctors d ON a.doctor_id = d.doctor_id
            CROSS JOIN appointment_stats stats
            WHERE date(a.date) >= {date_filter}
            ORDER BY a.date DESC
        """

    def _extract_entities(self, doc) -> Dict:
        """Extract entities and keywords from the text using spaCy."""
        entities = {
            "numbers": [],
            "dates": [],
            "names": [],
            "blood_types": []
        }
        
        # Extract numbers and dates
        for token in doc:
            if token.like_num:
                entities["numbers"].append(token.text)
            if token.ent_type_ == "DATE":
                entities["dates"].append(token.text)
                
        # Extract names (look for capitalized words that aren't at start of sentence)
        for token in doc:
            if (token.text[0].isupper() and 
                not token.is_sent_start and 
                token.text.lower() not in {"dr", "dr.", "doctor"}):
                entities["names"].append(token.text)
                
        # Look for blood types
        blood_type_pattern = re.compile(r'[ABO]+[+-]')
        for match in blood_type_pattern.finditer(doc.text):
            entities["blood_types"].append(match.group())
            
        return entities

    def _format_sql(self, sql: str) -> str:
        """Format SQL query for better readability."""
        return sqlparse.format(
            sql.strip(),
            reindent=True,
            keyword_case='upper',
            strip_comments=True
        )

    def _match_exact_count_query(self, text: str) -> Optional[str]:
        """Match exact count query patterns."""
        count_patterns = {
            "how many patients do we have?": "count_patients",
            "how many doctors are there?": "count_doctors",
            "how many appointments are there?": "count_appointments",
            "how many doctors are there in each specialty?": "count_doctors_by_specialty",
            "count total number of appointments": "count_appointments",
            "count total number of patients": "count_patients"
        }
        return count_patterns.get(text)

    def _match_blood_type_query(self, text: str) -> Optional[tuple]:
        """Match blood type query patterns."""
        logger.debug(f"Attempting to match blood type patterns in: {text}")
        patterns = [
            r"(?:blood\s+type|type)\s+([ABO][+-])",
            r"(?:with|having)\s+([ABO][+-])",
            r"list patients?\s+with\s+blood\s+type\s+([ABO][+-])"
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                blood_type = match.group(1).upper()
                logger.info(f"Found blood type: {blood_type}")
                return blood_type
        logger.debug("No blood type pattern matched")
        return None

    def _match_time_query(self, text: str) -> Optional[tuple]:
        """Match time-based query patterns."""
        if "appointment" not in text:
            return None
        pattern = r"(?:in |from |during |the |last |past |recent )?(\d+|a|the|this)?\s*(day|week|month)s?"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            number = match.group(1) or "1"
            unit = match.group(2)
            return (number, unit)
        return None

    def convert(self, text: str) -> Optional[str]:
        """Convert natural language question to SQL using flexible matching.
        
        Args:
            text: Natural language question about medical data
            
        Returns:
            SQL query string or None if no pattern matches
        """
        logger.info(f"Converting query: {text}")
        start_time = datetime.now()
        
        # Clean and normalize input
        text = text.strip().lower()
        logger.debug(f"Normalized text: {text}")
        
        # Step 1: Try exact count patterns
        pattern_name = self._match_exact_count_query(text)
        if pattern_name:
            return self._format_sql(self.patterns[pattern_name]["sql"](None))
            
        # Step 2: Try blood type patterns
        blood_type = self._match_blood_type_query(text)
        if blood_type:
            return self._format_sql(self.patterns["patients_by_blood"]["sql"](blood_type))
            
        # Step 3: Try time-based patterns
        time_params = self._match_time_query(text)
        if time_params:
            number, unit = time_params
            return self._format_sql(self.patterns["recent_appointments"]["sql"](number, unit))
            
        # Step 4: Try exact matches for common patterns
        exact_patterns = {
            "show all patients": "all_patients",
            "list all doctors and their specialties": "all_doctors",
            "show doctors who have the most appointments": "doctors_most_appointments"
        }
        pattern_name = exact_patterns.get(text)
        if pattern_name:
            return self._format_sql(self.patterns[pattern_name]["sql"](None))
            
        # Step 5: Try flexible pattern matching
        for pattern_name, pattern_info in self.patterns.items():
            match = re.search(pattern_info["pattern"], text, re.IGNORECASE)
            if not match:
                continue
                
            try:
                sql = None
                try:
                    if pattern_name == "doctor_appointments":
                        sql = pattern_info["sql"](match.group(1))
                        logger.info(f"Matched doctor appointments pattern for doctor: {match.group(1)}")
                    elif pattern_name == "patient_appointments":
                        sql = pattern_info["sql"](match.group(2))
                        logger.info(f"Matched patient appointments pattern for patient: {match.group(2)}")
                    elif pattern_name == "patient_age":
                        sql = pattern_info["sql"](match.group(1))
                        logger.info(f"Matched patient age pattern with age: {match.group(1)}")
                    elif pattern_name == "doctors_with_min_appointments":
                        threshold = int(re.search(r"more than (\d+)", text).group(1))
                        sql = pattern_info["sql"](threshold)
                        logger.info(f"Matched doctors with min appointments pattern, threshold: {threshold}")
                    else:
                        sql = pattern_info["sql"](None)
                        logger.info(f"Matched pattern: {pattern_name}")
                except Exception as e:
                    logger.error(f"Error processing pattern {pattern_name}: {str(e)}")
                    
                if sql:
                    return self._format_sql(sql)
            except Exception:
                continue
                
        # Step 6: Fallback for common entities
        if "patient" in text:
            return self._format_sql(self.patterns["all_patients"]["sql"](None))
        elif "doctor" in text:
            return self._format_sql(self.patterns["all_doctors"]["sql"](None))
        elif "appointment" in text:
            return self._format_sql(self.patterns["recent_appointments"]["sql"]("7", "days"))
            
        # Process text with spaCy for more complex queries
        doc = nlp(text)
        entities = self._extract_entities(doc)
        query_type = self._determine_query_type(text)
        
        # Try pattern-based matching
        match = self._match_query_pattern(text, query_type, entities, [])
        if match:
            return self._format_sql(match)
            
        return None
        """Convert natural language question to SQL query using NLP.
        
        Args:
            text: Natural language question about medical data
            
        Returns:
            SQL query string or None if no pattern matches
        """
        # Process text with spaCy
        doc = nlp(text.lower().strip())
        
        # Extract entities
        entities = self._extract_entities(doc)
        
        # Priority 1: Handle blood type queries
        blood_type_patterns = [
            r"(?:blood\s+type|type)\s+([ABO][+-])",
            r"(?:with|having)\s+([ABO][+-])",
            r"([ABO][+-])(?:\s+blood\s+type)?"
        ]
        
        for pattern in blood_type_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                blood_type = match.group(1)
                sql = self.patterns["patient_blood_type"]["sql"](blood_type)
                return self._format_sql(sql)
        
        # Priority 2: Handle threshold-based queries
        threshold_match = re.search(self.query_types["threshold"], text, re.IGNORECASE)
        threshold_value = int(threshold_match.group(1)) if threshold_match else None
        
        # Try each pattern
        for pattern_name, pattern_info in self.patterns.items():
            match = re.search(pattern_info["pattern"], text, re.IGNORECASE)
            if not match:
                continue
                
            try:
                sql = None
                
                if pattern_name == "recent_appointments":
                    sql = pattern_info["sql"](int(match.group(1)), match.group(2))
                elif pattern_name == "doctor_appointments":
                    sql = pattern_info["sql"](match.group(1))
                elif pattern_name == "patient_appointments":
                    sql = pattern_info["sql"](match.group(2))
                elif pattern_name == "patient_age":
                    sql = pattern_info["sql"](match.group(1))
                elif pattern_name == "doctors_with_min_appointments" and threshold_value:
                    sql = pattern_info["sql"](threshold_value)
                else:
                    sql = pattern_info["sql"](None)
                
                if sql:
                    return self._format_sql(sql)
                    
            except Exception as e:
                continue  # Try next pattern if current one fails
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"Query conversion completed in {execution_time:.2f} seconds")
        
        if sql:
            logger.debug(f"Generated SQL: {sql}")
            return self._format_sql(sql)
        else:
            logger.warning(f"No matching pattern found for query: {text}")
            return None
            
    def _determine_query_type(self, text: str) -> str:
        """Determine the type of query being asked."""
        if any(phrase in text for phrase in ["how many", "count", "number of"]):
            return "COUNT"
        elif any(phrase in text for phrase in ["show me", "list", "what are", "display"]):
            return "SELECT"
        elif any(phrase in text for phrase in ["average", "mean"]):
            return "AGGREGATE"
        else:
            return "SELECT"  # Default to SELECT
            
    def _extract_entities(self, doc) -> dict:
        """Extract relevant entities from the question."""
        entities = {
            "persons": [],
            "dates": [],
            "age": None,
            "gender": None,
            "conditions": []
        }
        
        # Extract person names
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                entities["persons"].append(ent.text)
        
        # Look for dates (including years)
        for token in doc:
            if token.like_num and len(token.text) == 4 and token.text.startswith("2"):
                entities["dates"].append(token.text)
                
        # Check for age references
        age_pattern = re.search(r"(\d+)\s*(?:year(?:s)?(?:\s+old)?|yo)", doc.text.lower())
        if age_pattern:
            entities["age"] = age_pattern.group(1)
            
        # Check for gender
        if "female" in doc.text.lower() or "women" in doc.text.lower():
            entities["gender"] = "F"
        elif "male" in doc.text.lower() or "men" in doc.text.lower():
            entities["gender"] = "M"
            
        return entities
        
    def _extract_conditions(self, doc) -> List[Tuple[str, str, str]]:
        """Extract conditions from the question."""
        conditions = []
        
        # Age conditions
        age_words = ["over", "under", "older", "younger"]
        for token in doc:
            if token.like_num:
                age_context = [t.text.lower() for t in token.children]
                age_head = token.head.text.lower()
                if any(word in age_context + [age_head] for word in age_words):
                    op = ">" if any(w in age_context + [age_head] for w in ["over", "older"]) else "<"
                    conditions.append(("age", op, token.text))
                    
        return conditions

    def _match_query_pattern(self, text: str, query_type: str, entities: dict, conditions: List[Tuple]) -> str:
        """Match question against common query patterns and generate SQL."""
        
        # 1. Appointment queries
        if self._is_appointment_query(text):
            return self._build_appointment_query(text, entities)
            
        # 2. Patient information queries
        elif self._is_patient_query(text):
            return self._build_patient_query(text, entities, conditions)
            
        # 3. Medication/prescription queries
        elif self._is_medication_query(text):
            return self._build_medication_query(text, entities)
            
        # 4. Count queries
        elif query_type == "COUNT":
            return self._build_count_query(text, entities, conditions)
            
        # 5. Generic list queries
        elif "list" in text or "show" in text:
            return self._build_list_query(text)
            
        return None
        
    def _is_appointment_query(self, text: str) -> bool:
        """Check if the question is about appointments."""
        return any(word in text for word in self.term_map["appointment"])
        
    def _is_patient_query(self, text: str) -> bool:
        """Check if the question is about patients."""
        return any(word in text for word in self.term_map["patient"])
        
    def _is_medication_query(self, text: str) -> bool:
        """Check if the question is about medications or prescriptions."""
        return any(word in text for word 
                  in self.term_map["medication"] + self.term_map["prescription"])

    def _build_appointment_query(self, text: str, entities: dict) -> str:
        """Build SQL for appointment-related queries."""
        # Base query using schema-defined keys
        query = f"""
            SELECT a.date, p.name as patient_name, d.name as doctor_name, a.reason
            FROM appointments a
            JOIN patients p ON a.patient_id = p.patient_id
            JOIN doctors d ON a.doctor_id = d.doctor_id
        """
        
        conditions = []
        
        # Add doctor/patient filters
        if entities["persons"]:
            for person in entities["persons"]:
                name = person["name"].replace("'", "''")
                if person.get("is_doctor"):
                    conditions.append(f"d.name LIKE '%{name}%'")
                else:
                    conditions.append(f"p.name LIKE '%{name}%'")
                
        # Add date filters
        if entities["dates"]:
            date = entities["dates"][0]
            if "-" in date:  # If it's a full date with month
                conditions.append(f"a.date LIKE '{date}%'")
            else:  # If it's just a year
                conditions.append(f"a.date LIKE '{date}%'")
            
        # Add WHERE clause if needed
        if conditions:
            query += "\nWHERE " + " AND ".join(conditions)
            
        # Add ordering
        query += "\nORDER BY a.date DESC"
        
        return query

    def _build_patient_query(self, text: str, entities: dict, conditions: List[Tuple]) -> str:
        """Build SQL for patient-related queries."""
        # Base query
        query = "SELECT name, age, gender FROM patients"
        
        where_clauses = []
        
        # Add age conditions
        for field, op, value in conditions:
            if field == "age":
                where_clauses.append(f"age {op} {value}")
                
        # Add gender condition
        if entities["gender"]:
            where_clauses.append(f"gender = '{entities['gender']}'")
            
        # Add WHERE clause if needed
        if where_clauses:
            query += "\nWHERE " + " AND ".join(where_clauses)
            
        return query

    def _build_medication_query(self, text: str, entities: dict) -> str:
        """Build SQL for medication/prescription-related queries."""
        query = f"""
            SELECT m.name as medication, p.dosage, p.date, pt.name as patient_name
            FROM prescriptions p
            JOIN medications m ON p.{self.keys['prescriptions']['foreign_keys']['med_id']['column']} = m.{self.keys['medications']['primary_key']}
            JOIN patients pt ON p.{self.keys['prescriptions']['foreign_keys']['patient_id']['column']} = pt.{self.keys['patients']['primary_key']}
        """
        
        conditions = []
        
        # Add patient filter
        if entities["persons"]:
            patient = entities["persons"][0].replace("'", "''")
            conditions.append(f"pt.name LIKE '%{patient}%'")
            
        # Add date filter
        if entities["dates"]:
            conditions.append(f"p.date LIKE '{entities['dates'][0]}%'")
            
        # Add WHERE clause if needed
        if conditions:
            query += "\nWHERE " + " AND ".join(conditions)
            
        # Add ordering
        query += "\nORDER BY p.date DESC"
        
        return query

    def _build_count_query(self, text: str, entities: dict, conditions: List[Tuple]) -> str:
        """Build SQL for count queries."""
        # Determine what we're counting based on the text
        for table, terms in self.term_map.items():
            if any(term in text.lower() for term in terms):
                count_target = table + "s"  # Add 's' for plural
                break
        else:
            count_target = "patients"  # Default to patients if no match
        
        # Build the base count query
        query = f"SELECT COUNT(*) as count FROM {count_target}"
        
        where_clauses = []
        
        # Add conditions based on the table we're counting
        if count_target == "patients":
            if entities["gender"]:
                where_clauses.append(f"gender = '{entities['gender']}'")
            for field, op, value in conditions:
                if field == "age":
                    where_clauses.append(f"age {op} {value}")
        
        # Add WHERE clause if we have conditions
        if where_clauses:
            query += "\nWHERE " + " AND ".join(where_clauses)
        
        return query

    def _build_list_query(self, text: str) -> str:
        """Build SQL for general list queries."""
        # Try to determine which table to query
        for table in self.schema:
            if table.lower() in text:
                return f"SELECT * FROM {table} LIMIT 100"
        
        return None

    def _handle_complex_query(self, analysis: Dict) -> str:
        """Handle complex queries with multiple components."""
        if "average" in analysis["metrics"]:
            return self._build_average_query(analysis)
        elif "most" in analysis["metrics"] or "least" in analysis["metrics"]:
            return self._build_superlative_query(analysis)
        else:
            return self._build_multi_table_query(analysis)
            
    def _build_average_query(self, analysis: Dict) -> str:
        """Build queries for averages (e.g., average age by specialty)."""
        base_table = self._identify_base_table(analysis)
        group_by = self._identify_group_by(analysis)
        
        query = f"""SELECT {group_by}, ROUND(AVG("""
        
        # Determine what we're averaging
        if "age" in analysis["entities"]:
            query += "p.age"
        else:
            query += "COUNT(*)"
            
        query += f"), 1) as average\nFROM {base_table} "
        
        # Add necessary joins
        query += self._build_joins(analysis)
        
        # Add grouping
        query += f"\nGROUP BY {group_by}"
        
        # Add ordering
        query += "\nORDER BY average DESC"
        
        return query
        
    def _build_superlative_query(self, analysis: Dict) -> str:
        """Build queries for finding maximum/minimum counts."""
        base_table = self._identify_base_table(analysis)
        count_target = self._identify_count_target(analysis)
        
        query = f"""SELECT {count_target}, COUNT(*) as count 
                   FROM {base_table} """
        
        # Add necessary joins
        query += self._build_joins(analysis)
        
        # Add grouping
        query += f"\nGROUP BY {count_target}"
        
        # Add ordering based on most/least
        direction = "DESC" if "most" in analysis["metrics"] else "ASC"
        query += f"\nORDER BY count {direction}"
        query += "\nLIMIT 1"
        
        return query
        
    def _build_multi_table_query(self, analysis: Dict) -> str:
        """Build complex queries involving multiple tables."""
        # For medication prescriptions for a specific patient
        if "medications" in str(analysis) and any(not p.get("is_doctor") for p in analysis["entities"].get("persons", [])):
            return self._build_patient_medications_query(analysis)
            
        # Start with base table and main columns
        base_table = self._identify_base_table(analysis)
        columns = self._identify_columns(analysis)
        
        query = f"SELECT {columns}\nFROM {base_table} "
        
        # Add necessary joins
        query += self._build_joins(analysis)
        
        # Add conditions
        conditions = self._build_conditions(analysis)
        if conditions:
            query += f"\nWHERE {conditions}"
            
        # Add any group by
        group_by = self._identify_group_by(analysis)
        if group_by:
            query += f"\nGROUP BY {group_by}"
            
        return query
        
    def _build_patient_medications_query(self, analysis: Dict) -> str:
        """Build query for patient's medications."""
        # Find the patient name
        patient_name = next(
            person["name"] for person in analysis["entities"].get("persons", [])
            if not person.get("is_doctor")
        )
        
        # Build the specific query for medications
        query = """
            SELECT m.name as medication, p.dosage, p.date
            FROM prescriptions p
            JOIN medications m ON p.med_id = m.med_id
            JOIN patients pa ON p.patient_id = pa.patient_id
            WHERE pa.name = ?
            ORDER BY p.date DESC
        """.replace("?", f"'{patient_name}'")
        
        return query
        
    def _build_joins(self, analysis: Dict) -> str:
        """Build necessary JOIN clauses based on analysis."""
        base_table = self._identify_base_table(analysis)
        joins = []
        tables_needed = set()
        
        # Add tables based on entities mentioned
        if analysis["entities"]["persons"]:
            for person in analysis["entities"]["persons"]:
                if person["is_doctor"]:
                    tables_needed.add("doctors")
                else:
                    tables_needed.add("patients")
                    
        if "medications" in str(analysis) or "prescriptions" in str(analysis):
            tables_needed.update(["medications", "prescriptions"])
            
        # Build JOIN clauses based on base table
        if base_table == "prescriptions":
            joins.extend([
                "JOIN medications m ON prescriptions.med_id = m.med_id",
                "JOIN patients pa ON prescriptions.patient_id = pa.patient_id"
            ])
        elif base_table == "appointments":
            joins.extend([
                "JOIN patients p ON appointments.patient_id = p.patient_id",
                "JOIN doctors d ON appointments.doctor_id = d.doctor_id"
            ])
        else:
            # Add other necessary joins based on context
            if "patients" in tables_needed and base_table != "patients":
                joins.append(f"JOIN patients p ON {base_table}.patient_id = p.patient_id")
            if "doctors" in tables_needed and base_table != "doctors":
                joins.append(f"JOIN doctors d ON {base_table}.doctor_id = d.doctor_id")
            if "medications" in tables_needed and base_table != "medications":
                joins.append(f"JOIN medications m ON prescriptions.med_id = m.med_id")
                               
        return "\n".join(joins)
        
    def _add_time_context(self, sql: str, time_context: Dict) -> str:
        """Add time-based conditions to a query."""
        if "WHERE" in sql:
            connector = "AND"
        else:
            connector = "WHERE"
            
        if time_context["unit"] == "month":
            sql += f"\n{connector} date >= date('now', '-1 month')"
        elif time_context["unit"] == "year":
            sql += f"\n{connector} date >= date('now', '-1 year')"
        elif time_context["unit"] == "week":
            sql += f"\n{connector} date >= date('now', '-7 days')"
            
        return sql
        
    def _identify_base_table(self, analysis: Dict) -> str:
        """Identify the main table for the query based on analysis."""
        # Check query focus
        if "medications" in analysis["metrics"] or any("medication" in str(e) for e in analysis["entities"]["persons"]):
            return "medications"
        
        for person in analysis["entities"].get("persons", []):
            if person.get("is_doctor"):
                return "doctors"
            else:
                if "prescriptions" in str(analysis):
                    return "prescriptions"
                elif "appointments" in str(analysis):
                    return "appointments"
                return "patients"
                
        # Default mappings based on common terms
        term_table_map = {
            "age": "patients",
            "specialty": "doctors",
            "appointment": "appointments",
            "prescription": "prescriptions",
            "medication": "medications"
        }
        
        for term, table in term_table_map.items():
            if term in str(analysis).lower():
                return table
                
        return "patients"  # Default to patients if no clear indication
        
    def _identify_columns(self, analysis: Dict) -> str:
        """Identify which columns to select based on analysis."""
        base_table = self._identify_base_table(analysis)
        
        if "metrics" in analysis and analysis["metrics"]:
            if "average" in analysis["metrics"]:
                return "ROUND(AVG(p.age), 1) as average_age"
            elif "count" in analysis["metrics"]:
                return "COUNT(*) as count"
                
        # Default columns based on table
        table_columns = {
            "patients": "p.name, p.age, p.gender",
            "doctors": "d.name, d.specialty",
            "appointments": "a.date, p.name as patient_name, d.name as doctor_name, a.reason",
            "medications": "m.name as medication, m.manufacturer",
            "prescriptions": "m.name as medication, p.dosage, p.date, pa.name as patient_name"
        }
        
        return table_columns.get(base_table, "*")
        
    def _identify_count_target(self, analysis: Dict) -> str:
        """Identify what we're counting in count queries."""
        base_table = self._identify_base_table(analysis)
        
        # Map tables to their typical count targets
        count_targets = {
            "patients": "p.name",
            "doctors": "d.name",
            "appointments": "a.date",
            "medications": "m.name",
            "prescriptions": "p.date"
        }
        
        return count_targets.get(base_table, "*")
        
    def _identify_group_by(self, analysis: Dict) -> str:
        """Identify GROUP BY clause based on analysis."""
        if "specialty" in str(analysis):
            return "d.specialty"
        elif any(person.get("is_doctor") for person in analysis["entities"].get("persons", [])):
            return "d.name, d.doctor_id"
        elif "medications" in str(analysis):
            return "m.name, m.med_id"
            
        return ""
        
    def _build_conditions(self, analysis: Dict) -> str:
        """Build WHERE clause conditions from analysis."""
        conditions = []
        
        # Handle person names
        for person in analysis["entities"].get("persons", []):
            name = person["name"].replace("'", "''")
            if person.get("is_doctor"):
                conditions.append(f"d.name LIKE '%{name}%'")
            else:
                conditions.append(f"pa.name = '{name}'")
                
        # Handle age conditions
        for condition in analysis.get("conditions", []):
            if condition["type"] == "age":
                conditions.append(f"p.age {condition['operator']} {condition['value']}")
                
        # Handle gender
        if analysis["entities"].get("gender"):
            conditions.append(f"p.gender = '{analysis['entities']['gender']}'")
            
        # Handle time context
        if analysis.get("time_context"):
            if analysis["time_context"]["unit"] == "month":
                conditions.append("p.date >= date('now', '-1 month')")
            elif analysis["time_context"]["unit"] == "year":
                conditions.append("p.date >= date('now', '-1 year')")
                
        return " AND ".join(conditions) if conditions else ""
        
    def schema_text(self) -> str:
        """Get a text representation of the database schema."""
        parts = []
        for table, cols in (self.schema or {}).items():
            parts.append(f"{table}({', '.join(cols)})")
        return "; ".join(parts)