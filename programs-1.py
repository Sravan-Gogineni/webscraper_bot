# import modules
import pandas as pd
from ddgs import DDGS
import time
import os
import json
import re
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, select, func
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
import google.generativeai as genai
from multiprocessing import Pool, Manager, cpu_count
from functools import partial
import sys

# Load environment variables
load_dotenv()

# Global configuration (will be set before processing)
API_KEY = None
PROGRAM_URLS_CACHE = None

def get_db_engine():
    """Create database engine for standalone script (SQL Server)."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    # Get timeout from environment or use default of 60 seconds
    connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "60"))
    login_timeout = int(os.getenv("DB_LOGIN_TIMEOUT", "60"))
    
    if not all([database, username, password]):
        print("Error: Database credentials not set. Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD.")
        return None
    
    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Connection Timeout={connection_timeout};"
        f"Login Timeout={login_timeout};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_params)}"
    
    try:
        # Create engine with connection timeout settings
        engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            connect_args={
                "timeout": connection_timeout,
            },
            # Set pool timeout as well
            pool_timeout=connection_timeout,
        )
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_all_colleges(engine):
    """Get all colleges from the database."""
    try:
        metadata = MetaData()
        # Only reflect the specific tables we need
        metadata.reflect(bind=engine, only=["College", "ContactInformation"])
        college_table = metadata.tables.get("College")
        contact_table = metadata.tables.get("ContactInformation")
        
        if college_table is None:
            print("Error: College table not found in database.")
            return []
        
        with engine.connect() as conn:
            # Join with ContactInformation to get WebsiteUrl
            if contact_table is not None:
                stmt = (
                    select(
                        college_table.c.CollegeID,
                        college_table.c.CollegeName,
                        contact_table.c.WebsiteUrl
                    )
                    .outerjoin(contact_table, contact_table.c.CollegeID == college_table.c.CollegeID)
                    .order_by(college_table.c.CollegeName)
                )
                rows = conn.execute(stmt).fetchall()
                return [(row.CollegeID, row.CollegeName, row.WebsiteUrl) for row in rows]
            else:
                stmt = select(college_table.c.CollegeID, college_table.c.CollegeName).order_by(college_table.c.CollegeName)
                rows = conn.execute(stmt).fetchall()
                return [(row.CollegeID, row.CollegeName, None) for row in rows]
    except Exception as e:
        print(f"Error fetching colleges: {e}")
        import traceback
        traceback.print_exc()
        return []

def check_college_has_programs(engine, college_id):
    """Check if a college already has programs in the database.
    Returns True if the college has at least one program, False otherwise."""
    if not engine or not college_id:
        return False
    
    try:
        metadata = MetaData()
        # Only reflect the specific table we need
        metadata.reflect(bind=engine, only=["ProgramDepartmentLink"])
        program_link_table = metadata.tables.get("ProgramDepartmentLink")
        
        if program_link_table is None:
            return False
        
        with engine.connect() as conn:
            # Count how many programs this college has
            count_stmt = select(func.count(program_link_table.c.LinkID)).where(
                program_link_table.c.CollegeID == college_id
            )
            count = conn.execute(count_stmt).scalar() or 0
            
            return count > 0
            
    except Exception as e:
        return False

def get_all_admissions_offices(engine, college_id):
    """Get all admissions offices (departments) for a college.
    Returns list of tuples (CollegeDepartmentID, DepartmentName)."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine, only=["CollegeDepartment", "Department"])
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        
        if college_department_table is None or department_table is None:
            return []
        
        with engine.connect() as conn:
            stmt = (
                select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                .where(college_department_table.c.CollegeID == college_id)
                .order_by(department_table.c.DepartmentName)
            )
            rows = conn.execute(stmt).fetchall()
            return [(row.CollegeDepartmentID, row.DepartmentName) for row in rows]
    except Exception as e:
        return []

def use_gemini_to_match_department(program_name, program_level, program_school, available_offices):
    """Use Gemini AI to intelligently match a program to the most appropriate admissions office.
    Returns (CollegeDepartmentID, DepartmentName) or (None, None)."""
    try:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            return None, None
        
        if not available_offices:
            return None, None
        
        genai.configure(api_key=api_key)
        
        # Try to get the model
        model = None
        model_candidates = ["gemini-3-pro-preview", "gemini-1.5-pro", "gemini-pro"]
        
        for candidate in model_candidates:
            try:
                model = genai.GenerativeModel(f"models/{candidate}")
                break
            except Exception:
                continue
        
        if model is None:
            return None, None
        
        # Format available offices for the prompt
        offices_list = [f"{idx + 1}. {dept_name} (ID: {dept_id})" for idx, (dept_id, dept_name) in enumerate(available_offices)]
        offices_text = "\n".join(offices_list)
        
        prompt = f"""You are helping to match a university program to the most appropriate admissions office.

PROGRAM INFORMATION:
- Program Name: {program_name}
- Program Level: {program_level}
- School/College: {program_school if program_school else 'Not specified'}

AVAILABLE ADMISSIONS OFFICES FOR THIS UNIVERSITY:
{offices_text}

TASK:
Determine which admissions office is most appropriate for this program. Consider:
1. If the program belongs to a specific school (e.g., "School of Health Sciences"), look for a school-specific admissions office (e.g., "School of Health Sciences Admissions")
2. If no school-specific office exists, use the general graduate/undergraduate admissions office based on the program level
3. Master's, Doctorate, PhD programs → Graduate Admissions
4. Bachelor's programs → Undergraduate Admissions

IMPORTANT RULES:
- Only match to school-specific admissions offices if the program's school clearly matches the office name
- Example: "Master of Health Science" in "School of Health Sciences" → "School of Health Sciences Admissions" (if available)
- Example: "Master of Business Administration" in "School of Business" → "School of Business Admissions" (if available)
- If no school-specific match, use general "Graduate Admissions" or "Undergraduate Admissions"
- NEVER match a Master's/Doctorate program to "Undergraduate Admissions"
- NEVER match a Bachelor's program to "Graduate Admissions"

Respond with ONLY the number (1, 2, 3, etc.) corresponding to the best match. If no good match exists, respond with "0"."""
        
        response = model.generate_content(prompt)
        
        if response and response.text:
            try:
                choice = int(response.text.strip())
                if 1 <= choice <= len(available_offices):
                    dept_id, dept_name = available_offices[choice - 1]
                    return dept_id, dept_name
            except (ValueError, IndexError):
                pass
        
        return None, None
        
    except Exception as e:
        return None, None

def find_college_department(engine, college_id, department_name, program_level=None, program_name=None, program_school=None):
    """Find CollegeDepartmentID by college and department name with multiple matching strategies.
    Falls back to program level-based matching if explicit department name doesn't match.
    Uses Gemini AI for school-specific matching when appropriate."""
    try:
        metadata = MetaData()
        # Only reflect the specific tables we need
        metadata.reflect(bind=engine, only=["CollegeDepartment", "Department"])
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        
        if college_department_table is None or department_table is None:
            return None
        
        # Get all available admissions offices for this college
        available_offices = get_all_admissions_offices(engine, college_id)
        
        dept_name_clean = None
        if department_name:
            # Clean department name - remove college name prefix if present
            dept_name_clean = department_name.strip()
            if "—" in dept_name_clean or "-" in dept_name_clean:
                parts = re.split(r"[—\-]", dept_name_clean, 1)
                dept_name_clean = parts[-1].strip()
            
            # Remove common prefixes
            dept_name_clean = re.sub(r'^(the|a|an)\s+', '', dept_name_clean, flags=re.IGNORECASE).strip()
        
        with engine.connect() as conn:
            # Strategy 1: Exact match (case-insensitive) - if we have department name
            if dept_name_clean:
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName) == func.upper(dept_name_clean))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
                
                # Strategy 2: Partial match (LIKE)
                search_pattern = f"%{dept_name_clean.upper()}%"
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName).like(search_pattern))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
                
                # Strategy 3: Try matching key words from department name
                dept_words = dept_name_clean.upper().split()
                for word in dept_words:
                    if len(word) > 3:  # Only search for words longer than 3 characters
                        word_pattern = f"%{word}%"
                        stmt = (
                            select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                            .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                            .where(
                                (college_department_table.c.CollegeID == college_id) &
                                (func.upper(department_table.c.DepartmentName).like(word_pattern))
                            )
                        )
                        result = conn.execute(stmt).first()
                        if result:
                            return result[0]
            
            # Strategy 4: Use Gemini to match school-specific admissions offices (if program has a school)
            if program_school and program_name and program_level and available_offices:
                # Check if there are any school-specific admissions offices
                school_specific_offices = [
                    (dept_id, dept_name) for dept_id, dept_name in available_offices
                    if "ADMISSIONS" in dept_name.upper() and any(
                        word in dept_name.upper() for word in program_school.upper().split()
                        if len(word) > 3  # Only meaningful words
                    )
                ]
                
                # If we have school-specific offices, use Gemini to match
                if school_specific_offices:
                    gemini_match = use_gemini_to_match_department(
                        program_name, program_level, program_school, school_specific_offices
                    )
                    if gemini_match[0]:
                        return gemini_match[0]
            
            # Strategy 5: Fallback - Infer based on program level if no explicit match found
            # Strategy 1: Exact match (case-insensitive) - if we have department name
            if dept_name_clean:
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName) == func.upper(dept_name_clean))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
                
                # Strategy 2: Partial match (LIKE)
                search_pattern = f"%{dept_name_clean.upper()}%"
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName).like(search_pattern))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
                
                # Strategy 3: Try matching key words from department name
                dept_words = dept_name_clean.upper().split()
                for word in dept_words:
                    if len(word) > 3:  # Only search for words longer than 3 characters
                        word_pattern = f"%{word}%"
                        stmt = (
                            select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                            .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                            .where(
                                (college_department_table.c.CollegeID == college_id) &
                                (func.upper(department_table.c.DepartmentName).like(word_pattern))
                            )
                        )
                        result = conn.execute(stmt).first()
                        if result:
                            return result[0]
            
            # Strategy 4: Fallback - Infer based on program level if no explicit match found
            if program_level:
                program_level_upper = program_level.upper().strip()
                
                # Determine if this is a graduate or undergraduate program
                # Be very explicit - graduate programs include Master, Doctorate, PhD, and Graduate Certificate
                is_graduate = any(level in program_level_upper for level in [
                    "MASTER", "MASTERS", "M.S.", "M.A.", "M.B.A.", "M.SC.", "M.ED.", "M.F.A.", 
                    "M.P.H.", "M.S.W.", "M.E.", "M.ENG", "MS", "MA", "MBA", "MSC", "MED", "MFA",
                    "DOCTORATE", "DOCTOR", "PHD", "PH.D.", "ED.D.", "D.PHIL", "DBA", "JD", "MD",
                    "GRADUATE CERTIFICATE", "GRAD CERTIFICATE", "GRAD CERT", "POSTGRADUATE",
                    "POST-GRADUATE", "POST GRADUATE", "GRADUATE"
                ])
                
                # Undergraduate programs include Bachelor, Associate, and Undergraduate Certificate
                is_undergraduate = any(level in program_level_upper for level in [
                    "BACHELOR", "BACHELORS", "B.S.", "B.A.", "B.SC.", "B.ED.", "B.F.A.", "B.B.A.",
                    "B.E.", "B.ENG", "BS", "BA", "BSC", "BED", "BFA", "BBA", "BE", "BENG",
                    "ASSOCIATE", "ASSOCIATES", "A.S.", "A.A.", "A.SC.", "A.ED.", "A.F.A.", "A.B.A.",
                    "A.E.", "A.ENG", "AS", "AA", "ASC", "AED", "AFA", "ABA", "AE", "AENG",
                    "UNDERGRADUATE CERTIFICATE", "UNDERGRAD CERTIFICATE", "UNDERGRAD CERT",
                    "UNDERGRADUATE"
                ])
                
                # Certificate programs need special handling - check if it's graduate or undergraduate certificate
                is_certificate = "CERTIFICATE" in program_level_upper
                is_graduate_certificate = is_certificate and (
                    "GRADUATE" in program_level_upper or 
                    "GRAD" in program_level_upper or
                    "POSTGRADUATE" in program_level_upper or
                    "POST-GRADUATE" in program_level_upper
                )
                is_undergraduate_certificate = is_certificate and (
                    "UNDERGRADUATE" in program_level_upper or
                    "UNDERGRAD" in program_level_upper
                )
                
                # Final determination
                is_graduate_final = is_graduate or is_graduate_certificate
                is_undergraduate_final = (is_undergraduate or is_undergraduate_certificate) and not is_graduate_final
                
                # CRITICAL: Graduate programs MUST only match graduate admissions offices
                if is_graduate_final:
                    graduate_patterns = [
                        "GRADUATE ADMISSIONS",
                        "GRADUATE SCHOOL ADMISSIONS",
                        "OFFICE OF GRADUATE ADMISSIONS",
                        "GRADUATE STUDIES ADMISSIONS",
                        "GRADUATE PROGRAMS ADMISSIONS",
                        "GRADUATE SCHOOL",
                        "GRADUATE STUDIES",
                        "GRADUATE PROGRAMS"
                    ]
                    
                    for pattern in graduate_patterns:
                        stmt = (
                            select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                            .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                            .where(
                                (college_department_table.c.CollegeID == college_id) &
                                (func.upper(department_table.c.DepartmentName).like(f"%{pattern}%")) &
                                (~func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%"))  # Explicitly exclude undergraduate
                            )
                        )
                        result = conn.execute(stmt).first()
                        if result:
                            return result[0]
                    
                    # More generic "GRADUATE" pattern (but still exclude undergraduate)
                    stmt = (
                        select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                        .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                        .where(
                            (college_department_table.c.CollegeID == college_id) &
                            (func.upper(department_table.c.DepartmentName).like("%GRADUATE%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%"))
                        )
                    )
                    result = conn.execute(stmt).first()
                    if result:
                        return result[0]
                
                # CRITICAL: Undergraduate programs MUST only match undergraduate admissions offices
                if is_undergraduate_final:
                    undergraduate_patterns = [
                        "UNDERGRADUATE ADMISSIONS",
                        "OFFICE OF UNDERGRADUATE ADMISSIONS",
                        "UNDERGRADUATE STUDIES ADMISSIONS",
                        "UNDERGRADUATE PROGRAMS ADMISSIONS",
                        "UNDERGRADUATE STUDIES",
                        "UNDERGRADUATE PROGRAMS"
                    ]
                    
                    for pattern in undergraduate_patterns:
                        stmt = (
                            select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                            .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                            .where(
                                (college_department_table.c.CollegeID == college_id) &
                                (func.upper(department_table.c.DepartmentName).like(f"%{pattern}%")) &
                                (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%"))  # Explicitly exclude graduate
                            )
                        )
                        result = conn.execute(stmt).first()
                        if result:
                            return result[0]
                    
                    # More generic "UNDERGRADUATE" pattern (but still exclude graduate)
                    stmt = (
                        select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                        .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                        .where(
                            (college_department_table.c.CollegeID == college_id) &
                            (func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%"))
                        )
                    )
                    result = conn.execute(stmt).first()
                    if result:
                        return result[0]
                    
                    # Last resort: generic "ADMISSIONS" (ONLY for undergraduate, and MUST exclude graduate)
                    stmt = (
                        select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                        .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                        .where(
                            (college_department_table.c.CollegeID == college_id) &
                            (func.upper(department_table.c.DepartmentName).like("%ADMISSIONS%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%GRAD%"))
                        )
                    )
                    result = conn.execute(stmt).first()
                    if result:
                        return result[0]
            
            return None
            
    except Exception as e:
        return None

def parse_json_response(text):
    """Parse JSON from Gemini response, handling markdown code blocks."""
    text = text.strip()
    
    # Extract JSON if it's wrapped in markdown code blocks
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        return None

def parse_date(date_str):
    """Parse date string to datetime2 format."""
    if not date_str:
        return None
    try:
        # Try common date formats
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return datetime.strptime(date_str[:10], fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None

def convert_bool(value):
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', 'yes', '1', 'required']
    if isinstance(value, (int, float)):
        return bool(value)
    return False

def save_program(engine, college_id, program_data):
    """Save program and all related data to database."""
    try:
        metadata = MetaData()
        # Only reflect the specific tables we need
        metadata.reflect(bind=engine, only=[
            "Program",
            "ProgramRequirements",
            "ProgramTermDetails",
            "ProgramTestScores",
            "ProgramDepartmentLink"
        ])
        
        program_table = metadata.tables.get("Program")
        program_req_table = metadata.tables.get("ProgramRequirements")
        program_term_table = metadata.tables.get("ProgramTermDetails")
        program_test_table = metadata.tables.get("ProgramTestScores")
        program_link_table = metadata.tables.get("ProgramDepartmentLink")
        
        if program_table is None:
            return False
        
        snapshot = program_data.get("Program Snapshot", {})
        checklist = program_data.get("Application Checklist", {})
        term_data = program_data.get("Term & Investment", {})
        dept_placement = program_data.get("Department Placement", {})
        test_scores = program_data.get("Minimum Test Scores", {})
        
        program_name = snapshot.get("Program Name")
        if not program_name:
            return False
        
        with engine.begin() as conn:
            # Check if program already exists (by name and level)
            level = snapshot.get("Level", "")
            existing = conn.execute(
                select(program_table.c.ProgramID)
                .where(
                    (func.upper(program_table.c.ProgramName) == func.upper(program_name)) &
                    (program_table.c.Level == level)
                )
            ).first()
            
            if existing:
                program_id = existing[0]
                # Update existing program
                program_values = {
                    "Concentration": snapshot.get("Concentration"),
                    "Description": snapshot.get("Description"),
                    "ProgramWebsiteURL": snapshot.get("Program Website URL"),
                    "Accreditation": snapshot.get("Accreditation"),
                    "QsWorldRanking": snapshot.get("Qs World Ranking"),
                    "School": snapshot.get("School"),
                }
                program_values = {k: v for k, v in program_values.items() if v is not None}
                if program_values:
                    conn.execute(
                        program_table.update()
                        .where(program_table.c.ProgramID == program_id)
                        .values(**program_values)
                    )
            else:
                # Insert new program
                program_values = {
                    "ProgramName": program_name,
                    "Level": level,
                    "Concentration": snapshot.get("Concentration"),
                    "Description": snapshot.get("Description"),
                    "ProgramWebsiteURL": snapshot.get("Program Website URL"),
                    "Accreditation": snapshot.get("Accreditation"),
                    "QsWorldRanking": snapshot.get("Qs World Ranking"),
                    "School": snapshot.get("School"),
                }
                program_values = {k: v for k, v in program_values.items() if v is not None}
                result = conn.execute(program_table.insert().values(**program_values))
                program_id = result.inserted_primary_key[0]
            
            # Save ProgramRequirements
            if program_req_table is not None and checklist:
                req_values = {
                    "ProgramID": program_id,
                    "Resume": "Required" if convert_bool(checklist.get("Resume")) else "Not Required",
                    "StatementOfPurpose": "Required" if convert_bool(checklist.get("Statement Of Purpose")) else "Not Required",
                    "GreOrGmat": "Required" if convert_bool(checklist.get("Gre Or Gmat")) else "Not Required",
                    "EnglishScore": checklist.get("English Score"),
                    "Requirements": checklist.get("Requirements"),
                    "WritingSample": "Required" if convert_bool(checklist.get("Writing Sample")) else "Not Required",
                    "IsAnalyticalNotRequired": convert_bool(checklist.get("Is Analytical Not Required")),
                    "IsAnalyticalOptional": convert_bool(checklist.get("Is Analytical Optional")),
                    "IsDuoLingoRequired": convert_bool(checklist.get("Is Duo Lingo Required")),
                    "IsELSRequired": convert_bool(checklist.get("Is E L S Required")),
                    "IsGMATOrGreRequired": convert_bool(checklist.get("Is G M A T Or Gre Required")),
                    "IsGMATRequired": convert_bool(checklist.get("Is G M A T Required")),
                    "IsGreRequired": convert_bool(checklist.get("Is Gre Required")),
                    "IsIELTSRequired": convert_bool(checklist.get("Is I E L T S Required")),
                    "IsLSATRequired": convert_bool(checklist.get("Is L S A T Required")),
                    "IsMATRequired": convert_bool(checklist.get("Is M A T Required")),
                    "IsMCATRequired": convert_bool(checklist.get("Is M C A T Required")),
                    "IsPTERequired": convert_bool(checklist.get("Is P T E Required")),
                    "IsTOEFLIBRequired": convert_bool(checklist.get("Is T O E F L I B Required")),
                    "IsTOEFLPBTRequired": convert_bool(checklist.get("Is T O E F L P B T Required")),
                    "IsEnglishNotRequired": convert_bool(checklist.get("Is English Not Required")),
                    "IsEnglishOptional": convert_bool(checklist.get("Is English Optional")),
                    "IsRecommendationSystemOpted": convert_bool(checklist.get("Is Recommendation System Opted")),
                    "IsStemProgram": convert_bool(checklist.get("Is Stem Program")),
                    "IsACTRequired": convert_bool(checklist.get("Is A C T Required")),
                    "IsSATRequired": convert_bool(checklist.get("Is S A T Required")),
                    "MaxFails": checklist.get("Max Fails"),
                    "MaxGPA": checklist.get("Max G P A"),
                    "MinGPA": checklist.get("Min G P A"),
                    "PreviousYearAcceptanceRates": checklist.get("Previous Year Acceptance Rates"),
                }
                req_values = {k: v for k, v in req_values.items() if v is not None or k in ["IsAnalyticalNotRequired", "IsAnalyticalOptional", "IsDuoLingoRequired", "IsELSRequired", "IsGMATOrGreRequired", "IsGMATRequired", "IsGreRequired", "IsIELTSRequired", "IsLSATRequired", "IsMATRequired", "IsMCATRequired", "IsPTERequired", "IsTOEFLIBRequired", "IsTOEFLPBTRequired", "IsEnglishNotRequired", "IsEnglishOptional", "IsRecommendationSystemOpted", "IsStemProgram", "IsACTRequired", "IsSATRequired"]}
                
                existing_req = conn.execute(
                    select(program_req_table.c.ProgramReqID).where(program_req_table.c.ProgramID == program_id)
                ).first()
                
                if existing_req:
                    conn.execute(
                        program_req_table.update()
                        .where(program_req_table.c.ProgramID == program_id)
                        .values(**req_values)
                    )
                else:
                    conn.execute(program_req_table.insert().values(**req_values))
            
            # Save ProgramTermDetails - handle multiple terms
            if program_term_table is not None:
                # Check if term_data is a list (multiple terms) or single object
                terms_list = []
                if isinstance(term_data, list):
                    terms_list = term_data
                elif term_data and isinstance(term_data, dict):
                    terms_list = [term_data]
                
                for term_item in terms_list:
                    if not isinstance(term_item, dict):
                        continue
                    
                    term = term_item.get("Term")
                    if term:
                        term_values = {
                            "CollegeID": college_id,
                            "ProgramID": program_id,
                            "Term": term,
                            "LiveDate": parse_date(term_item.get("Live Date")),
                            "DeadlineDate": parse_date(term_item.get("Deadline Date")),
                            "Fees": str(term_item.get("Fees")) if term_item.get("Fees") else None,
                            "AverageScholarshipAmount": str(term_item.get("Average Scholarship Amount")) if term_item.get("Average Scholarship Amount") else None,
                            "CostPerCredit": str(term_item.get("Cost Per Credit")) if term_item.get("Cost Per Credit") else None,
                            "ScholarshipAmount": str(term_item.get("Scholarship Amount")) if term_item.get("Scholarship Amount") else None,
                            "ScholarshipPercentage": str(term_item.get("Scholarship Percentage")) if term_item.get("Scholarship Percentage") else None,
                            "ScholarshipType": term_item.get("Scholarship Type"),
                        }
                        term_values = {k: v for k, v in term_values.items() if v is not None or k in ["CollegeID", "ProgramID", "Term"]}
                        
                        existing_term = conn.execute(
                            select(program_term_table.c.ProgramTermID).where(
                                (program_term_table.c.CollegeID == college_id) &
                                (program_term_table.c.ProgramID == program_id) &
                                (program_term_table.c.Term == term)
                            )
                        ).first()
                        
                        if existing_term:
                            conn.execute(
                                program_term_table.update()
                                .where(program_term_table.c.ProgramTermID == existing_term[0])
                                .values(**term_values)
                            )
                        else:
                            conn.execute(program_term_table.insert().values(**term_values))
            
            # Save ProgramTestScores
            if program_test_table is not None and test_scores:
                test_values = {
                    "ProgramID": program_id,
                    "MinimumACTScore": str(test_scores.get("Minimum A C T Score")) if test_scores.get("Minimum A C T Score") else None,
                    "MinimumDuoLingoScore": str(test_scores.get("Minimum Duo Lingo Score")) if test_scores.get("Minimum Duo Lingo Score") else None,
                    "MinimumELSScore": str(test_scores.get("Minimum E L S Score")) if test_scores.get("Minimum E L S Score") else None,
                    "MinimumGMATScore": str(test_scores.get("Minimum G M A T Score")) if test_scores.get("Minimum G M A T Score") else None,
                    "MinimumGreScore": str(test_scores.get("Minimum Gre Score")) if test_scores.get("Minimum Gre Score") else None,
                    "MinimumIELTSScore": str(test_scores.get("Minimum I E L T S Score")) if test_scores.get("Minimum I E L T S Score") else None,
                    "MinimumMATScore": str(test_scores.get("Minimum M A T Score")) if test_scores.get("Minimum M A T Score") else None,
                    "MinimumMCATScore": str(test_scores.get("Minimum M C A T Score")) if test_scores.get("Minimum M C A T Score") else None,
                    "MinimumPTEScore": str(test_scores.get("Minimum P T E Score")) if test_scores.get("Minimum P T E Score") else None,
                    "MinimumSATScore": str(test_scores.get("Minimum S A T Score")) if test_scores.get("Minimum S A T Score") else None,
                    "MinimumTOEFLScore": str(test_scores.get("Minimum T O E F L Score")) if test_scores.get("Minimum T O E F L Score") else None,
                    "MinimumLSATScore": str(test_scores.get("Minimum L S A T Score")) if test_scores.get("Minimum L S A T Score") else None,
                }
                test_values = {k: v for k, v in test_values.items() if v is not None or k == "ProgramID"}
                
                existing_test = conn.execute(
                    select(program_test_table.c.TestScoreID).where(program_test_table.c.ProgramID == program_id)
                ).first()
                
                if existing_test:
                    conn.execute(
                        program_test_table.update()
                        .where(program_test_table.c.ProgramID == program_id)
                        .values(**test_values)
                    )
                else:
                    conn.execute(program_test_table.insert().values(**test_values))
            
            # Save ProgramDepartmentLink
            if program_link_table is not None:
                dept_name = None
                if dept_placement:
                    dept_name = dept_placement.get("College Department I D") or dept_placement.get("College Department ID") or dept_placement.get("Department Name")
                
                # Try to find department - first with explicit name, then fallback to program level and school
                program_school = snapshot.get("School")
                college_dept_id = None
                if dept_name:
                    # Try with explicit department name first
                    college_dept_id = find_college_department(engine, college_id, dept_name, level, program_name, program_school)
                
                # If no match found and we have a program level, try fallback based on level and school
                if not college_dept_id and level:
                    college_dept_id = find_college_department(engine, college_id, None, level, program_name, program_school)
                
                # Create link if department was found
                if college_dept_id:
                    existing_link = conn.execute(
                        select(program_link_table.c.LinkID).where(
                            (program_link_table.c.CollegeID == college_id) &
                            (program_link_table.c.ProgramID == program_id)
                        )
                    ).first()
                    
                    if existing_link:
                        conn.execute(
                            program_link_table.update()
                            .where(program_link_table.c.LinkID == existing_link[0])
                            .values(CollegeDepartmentID=college_dept_id)
                        )
                    else:
                        conn.execute(
                            program_link_table.insert().values(
                                CollegeID=college_id,
                                ProgramID=program_id,
                                CollegeDepartmentID=college_dept_id
                            )
                        )
        
        return True
        
    except Exception as e:
        return False

def find_matching_cache_entry(college_name, program_urls_cache):
    """Find matching cache entry for a college name."""
    grad_programs_url = None
    undergrad_programs_url = None
    matched_cache_name = None
    
    # Common generic words to ignore
    common_generic_words = {'university', 'college', 'school', 'institute', 'institution', 'academy', 'center', 'centre'}
    college_name_lower = college_name.lower().strip()
    
    # First try exact match
    for cached_name, cached_data in program_urls_cache.items():
        cached_name_lower = cached_name.lower().strip()
        if college_name_lower == cached_name_lower:
            matched_cache_name = cached_name
            grad_programs_url = cached_data.get("Graduate Programs URL")
            undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
            break
    
    # If no exact match, try partial matches
    if not matched_cache_name:
        college_words = set([w for w in college_name_lower.split() 
                            if len(w) > 3 and w not in common_generic_words])
        
        best_match = None
        best_score = 0
        
        for cached_name, cached_data in program_urls_cache.items():
            cached_name_lower = cached_name.lower().strip()
            cached_words = set([w for w in cached_name_lower.split() 
                               if len(w) > 3 and w not in common_generic_words])
            
            if college_words and cached_words:
                common_words = college_words & cached_words
                if common_words:
                    score = len(common_words) / max(len(college_words), len(cached_words))
                    if score > best_score and score >= 0.6:
                        best_score = score
                        best_match = (cached_name, cached_data)
        
        if best_match:
            matched_cache_name, cached_data = best_match
            grad_programs_url = cached_data.get("Graduate Programs URL")
            undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
    
    # If still no match, try fuzzy string matching
    if not matched_cache_name:
        for cached_name, cached_data in program_urls_cache.items():
            cached_name_lower = cached_name.lower().strip()
            if college_name_lower in cached_name_lower or cached_name_lower in college_name_lower:
                college_meaningful = ' '.join([w for w in college_name_lower.split() 
                                               if w not in common_generic_words])
                cached_meaningful = ' '.join([w for w in cached_name_lower.split() 
                                              if w not in common_generic_words])
                
                if college_meaningful and cached_meaningful:
                    if college_meaningful in cached_meaningful or cached_meaningful in college_meaningful:
                        if len(college_meaningful) >= 8 and len(cached_meaningful) >= 8:
                            matched_cache_name = cached_name
                            grad_programs_url = cached_data.get("Graduate Programs URL")
                            undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
                            break
    
    # Filter out None/null URLs
    if grad_programs_url and (grad_programs_url.lower() == 'null' or not grad_programs_url.strip()):
        grad_programs_url = None
    if undergrad_programs_url and (undergrad_programs_url.lower() == 'null' or not undergrad_programs_url.strip()):
        undergrad_programs_url = None
    
    return matched_cache_name, grad_programs_url, undergrad_programs_url

def get_prompt(college_name, url_type):
    """Generate the scraping prompt for a given URL type."""
    if url_type == "Graduate":
        program_focus = "GRADUATE programs (Master's, Doctorate/PhD, Graduate Certificates)"
        level_hint = "These should be Master's, Doctorate, or Graduate Certificate level programs"
    elif url_type == "Undergraduate":
        program_focus = "UNDERGRADUATE programs (Bachelor's degrees, Undergraduate Certificates)"
        level_hint = "These should be Bachelor's or Undergraduate Certificate level programs"
    else:
        program_focus = "ALL programs (both Undergraduate and Graduate)"
        level_hint = "Include programs of all levels"
    
    # Full prompt from the original file
    prompt = f"""You are a higher education data scraper. You are given a specific program listing page URL from a university.

🔥 CURRENT UNIVERSITY YOU ARE SCRAPING: "{college_name}" 🔥

This page specifically lists {program_focus}. You MUST scrape information for EVERY SINGLE PROGRAM listed on this page and any linked pages.

⚠️⚠️⚠️ CRITICAL: The school/college names you find MUST be specific to "{college_name}" - DO NOT use school names from other universities like "University of New Haven" or any other university. Only use school names that actually appear on "{college_name}"'s website.

CRITICAL REQUIREMENT: You MUST find and return ALL programs from this page - DO NOT limit yourself to just 5, 10, or any small number. This page likely lists dozens or hundreds of programs, and you need to find EVERY ONE OF THEM.

You MUST find ALL {program_focus} listed on this page, including:
- Every program from every school/college listed
- Every program from every department listed
- Every concentration and specialization
- Every certificate program
- Every online program
- Every professional program

IMPORTANT: 
- This is a program listing page, so explore ALL programs listed
- If there are links to individual program pages, follow them to get complete information
- If there are "View All" or "See More" links, follow them
- If there is pagination, explore ALL pages
- {level_hint}
- Each school/college section may have multiple programs - get them ALL
- Each department may have multiple programs - get them ALL

⚠️⚠️⚠️ CRITICAL - UNDERSTANDING SCHOOL vs DEPARTMENT ⚠️⚠️⚠️:

🔥🔥🔥 YOU ARE CURRENTLY SCRAPING: "{college_name}" 🔥🔥🔥

⚠️ CRITICAL DISTINCTION - "School" and "Department" are COMPLETELY DIFFERENT:

1. "School" = Organizational/Academic unit within the university
   - Examples: "School of Engineering", "School of Business", "College of Medicine", "Tagliatela College of Engineering"
   - This is where the program is offered/academically housed
   - Goes in the "School" field in Program Snapshot

2. "Department" = Admissions Office that handles applications
   - Examples: "Graduate Admissions", "Undergraduate Admissions", "Office of Graduate Admissions"
   - This is the office that processes applications for the program
   - Goes in the "Department Placement" section

DO NOT confuse these two - they serve different purposes!

🔥 VERY IMPORTANT: Each university has its own unique school/college structure. School names from one university are COMPLETELY DIFFERENT from another university. 

For example:
- "University of New Haven" has school names like: "Tagliatela College of Engineering", "Pompea College of Business", "Henry C. Lee College of Criminal Justice and Forensic Sciences"
- "{college_name}" will have COMPLETELY DIFFERENT school names (whatever that university actually calls its schools)
- DO NOT use "Tagliatela College of Engineering" or "Pompea College of Business" or any other University of New Haven school names when scraping "{college_name}"
- Each university's website will list its own unique school/college names

⚠️⚠️⚠️ CRITICAL RULES - READ CAREFULLY:
1. You are scraping "{college_name}" - find school names that are SPECIFIC to "{college_name}" only
2. DO NOT copy school names from examples in this prompt (they are just examples from other universities)
3. DO NOT use school names from "University of New Haven" or any other university you've seen before
4. DO NOT assume school names based on program type
5. ONLY use school names that are EXPLICITLY STATED on "{college_name}"'s website
6. If you cannot find the school name clearly stated on "{college_name}"'s website, use null
7. It is BETTER to leave it null than to guess or copy from examples or other universities

IMPORTANT: "School" here refers to the ORGANIZATIONAL UNIT WITHIN "{college_name}" (like "School of Medicine", "College of Engineering", "School of Business"), NOT the university name itself.

⚠️ CRITICAL - EACH UNIVERSITY HAS ITS OWN UNIQUE SCHOOL STRUCTURE:
Every university has its own unique school/college names. The school names for "University of New Haven" (like "Tagliatela College of Engineering") are DIFFERENT from "{college_name}"'s school names. 

You MUST find the school names that are SPECIFIC to "{college_name}" only. DO NOT copy school names from examples or from other universities like "University of New Haven".

Examples of what "School" means (these are GENERIC examples - each university will have its own unique names):
  * Medical programs → Could be "School of Medicine", "College of Medicine", "Faculty of Medicine", or whatever the university calls it (if stated on website)
  * Engineering programs → Could be "College of Engineering", "School of Engineering", "Faculty of Engineering", or whatever the university calls it (if stated on website)
  * Business programs → Could be "School of Business", "College of Business", "Business School", or whatever the university calls it (if stated on website)
  * Arts programs → Could be "College of Arts", "School of Arts", "Faculty of Arts", or whatever the university calls it (if stated on website)

⚠️ DO NOT use school names from other universities - only use names that appear on the CURRENT university's website you are scraping.

This is the ORGANIZATIONAL UNIT that offers the program, not:
  ❌ The university name (e.g., "University of New Haven")
  ❌ The department name (e.g., "Computer Science Department")
  ❌ The program name itself (e.g., "Master of Science in Computer Science")
  ❌ Guessed or assumed names based on program type
  ❌ Generic names you invent (e.g., don't assume "College of Engineering" just because it's an engineering program)

Look for the school/college name in:
  * Page headers and section titles (e.g., "College of Engineering Programs", "School of Business")
  * Breadcrumb navigation paths (e.g., "Home > Colleges > [School Name] > Programs") - use the actual school name that appears in the breadcrumb
  * Program detail pages that mention the parent school/college
  * Department listings that show which school they belong to
  * Menu navigation showing the organizational structure
  * URL structure that may indicate the school (e.g., /medicine/programs, /engineering/programs, /business/degrees)
  * Program listings organized under school/college sections

⚠️ MANDATORY ACCURACY RULES:
  * Use the EXACT, FULL name as it appears on the website - copy it word-for-word
  * DO NOT guess school names based on program type
  * DO NOT assume a program belongs to a school just because of its name
  * DO NOT create generic school names (e.g., don't say "College of Engineering" just because it's engineering)
  * DO NOT use abbreviations unless that's exactly how it appears on the website
  * DO NOT use the university name - only use the school/college within the university
  * If you cannot find the school name EXPLICITLY stated on the website, use null
  * It is BETTER to use null than to guess or fabricate a school name
  * Only use a school name if you can see it clearly written on the website
  * Each university has its own unique school names - find the names that are SPECIFIC to the university you are currently scraping
  * DO NOT copy school names from examples or from other universities
  * Only use school names that appear on the CURRENT university's website

DO NOT STOP after finding a few programs. This page exists specifically to list programs, so there should be many programs listed. Get EVERY SINGLE ONE.

For EACH program you find, you need to scrape the following information and return it in a structured JSON format as an array of program objects. Each program should have these sections:

1. "Program Snapshot":
   - "Program Name": Full name of the program (e.g., "Master of Science in Computer Science")
   - "Level": Program level (e.g., "Bachelor", "Master", "Doctorate", "Certificate")
   - "Concentration": Specialization or concentration (if applicable, otherwise null)
   - "Description": Description of the program
   - "Program Website URL": URL to the program's webpage
   - "Accreditation": Accreditation information
   - "Qs World Ranking": QS World Ranking if available
   - "School": The SCHOOL/COLLEGE organizational unit WITHIN THE UNIVERSITY that this program belongs to (NOT the university name itself). 
   
   ⚠️ IMPORTANT: "School" is DIFFERENT from "Department". They are completely different concepts:
   - "School" = Organizational unit like "School of Engineering", "School of Business", "College of Medicine", "Tagliatela College of Engineering"
   - "Department" (in Department Placement below) = Admissions office like "Graduate Admissions", "Undergraduate Admissions" - this is for admissions/application processing
   
   "School" refers to the organizational/academic unit within the university, such as:
   - "School of Medicine"
   - "School of Engineering" 
   - "College of Engineering"
   - "School of Business"
   - "College of Business"
   - "School of Arts and Sciences"
   - etc. 

   🔥🔥🔥 CRITICAL WARNING - DO NOT COPY SCHOOL NAMES FROM EXAMPLES OR OTHER UNIVERSITIES 🔥🔥🔥:
   
   EACH UNIVERSITY HAS ITS OWN UNIQUE SCHOOL NAMES. If you are scraping "Academy of Europe Arts", you MUST find the school names that are SPECIFIC to "Academy of Europe Arts" - DO NOT use school names like "Tagliatela College of Engineering" or "Pompea College of Business" (those are for University of New Haven, not Academy of Europe Arts).
   
   ⚠️ CRITICAL RULES:
   - Each university has DIFFERENT school names - find the names on the CURRENT university's website
   - DO NOT copy school names from examples in this prompt
   - DO NOT use school names from other universities you've seen before  
   - DO NOT guess or assume school names
   - Only use school names that are EXPLICITLY STATED on the CURRENT university's website
   - If you cannot find the school name clearly written on the website, use null
   - It is BETTER to leave it null than to guess incorrectly or copy from examples

   ⚠️ IMPORTANT: The examples below are GENERIC examples. Each university has its own unique school names. You must find the school names that are SPECIFIC to the university you are currently scraping. DO NOT copy school names from examples.
   
   Generic examples of school structures (but use the actual names from the current university's website):
   - "School of Medicine", "College of Medicine", "Faculty of Medicine"
   - "School of Engineering", "College of Engineering", "Faculty of Engineering"  
   - "School of Business", "College of Business", "Business School"
   - "College of Arts and Sciences", "School of Arts", "Faculty of Arts"
   - "School of Law", "College of Law"
   - "School of Nursing", "College of Nursing"
   
   Each university will have its own unique names - you must find and use the EXACT names that appear on that university's website.

   Rules:
   - Use the EXACT, FULL name as it appears on the website - copy it word-for-word
   - Look in page headers, breadcrumbs, navigation menus, section titles, and program detail pages
   - Do NOT use the university name - only use the school/college within the university
   - Do NOT guess based on program type (e.g., don't assume "College of Engineering" just because it's an engineering program)
   - Do NOT create generic names - only use names that are explicitly written on the website
   - If the school name is not clearly visible on the website, use null
   - Accuracy is more important than completeness - use null rather than making something up

2. "Application Checklist":
   - "Resume": true/false - Is resume required?
   - "Statement Of Purpose": true/false - Is statement of purpose required?
   - "Gre Or Gmat": true/false - Is GRE or GMAT required?
   - "English Score": true/false - Is English test score required?
   - "Requirements": General requirements as text
   - "Writing Sample": true/false - Is writing sample required?
   - "Is Analytical Not Required": true/false
   - "Is Analytical Optional": true/false
   - "Is Duo Lingo Required": true/false
   - "Is E L S Required": true/false
   - "Is G M A T Or Gre Required": true/false
   - "Is G M A T Required": true/false
   - "Is Gre Required": true/false
   - "Is I E L T S Required": true/false
   - "Is L S A T Required": true/false
   - "Is M A T Required": true/false
   - "Is M C A T Required": true/false
   - "Is P T E Required": true/false
   - "Is T O E F L I B Required": true/false
   - "Is T O E F L P B T Required": true/false
   - "Is English Not Required": true/false
   - "Is English Optional": true/false
   - "Is Recommendation System Opted": true/false
   - "Is Stem Program": true/false
   - "Is A C T Required": true/false
   - "Is S A T Required": true/false
   - "Max Fails": Maximum number of failed courses (if applicable)
   - "Max G P A": Maximum GPA (if applicable)
   - "Min G P A": Minimum GPA required
   - "Previous Year Acceptance Rates": Acceptance rate (if available)

3. "Term & Investment": 
   This can be a single object for one term OR an array of objects if the program has multiple terms (e.g., Fall, Spring, Summer).
   Each term object should have:
   - "Term": Term name (e.g., "Fall", "Spring", "Summer")
   - "Live Date": Application live date (YYYY-MM-DD format)
   - "Deadline Date": Application deadline date (YYYY-MM-DD format)
   - "Fees": Tuition fees (number)
   - "Average Scholarship Amount": Average scholarship amount
   - "Cost Per Credit": Cost per credit hour
   - "Scholarship Amount": Scholarship amount
   - "Scholarship Percentage": Scholarship percentage
   - "Scholarship Type": Type of scholarship

4. "Department Placement":
   ⚠️⚠️⚠️ CRITICAL DISTINCTION: "Department" here is COMPLETELY DIFFERENT from "School" above! ⚠️⚠️⚠️
   
   - "School" (above) = Organizational/academic unit like "School of Engineering", "School of Business"
   - "Department" (here) = ADMISSIONS OFFICE that handles applications for this program
   
   "Department Placement" refers to the ADMISSIONS OFFICE that processes applications for this program, such as:
   - "Graduate Admissions" (admissions office for graduate programs)
   - "Undergraduate Admissions" (admissions office for undergraduate programs)
   - "Graduate School Admissions" 
   - "Office of Graduate Admissions"
   - "Admissions Office"
   - etc.
   
   - "College Department I D": The exact admissions office/department name that handles applications for this program.
   
   ⚠️ CRITICAL: Only use admissions office names that are EXPLICITLY STATED on the website. DO NOT guess, assume, or fabricate admissions office names. If you cannot find the admissions office name clearly written on the website, use null. It is BETTER to use null than to guess incorrectly.
   
   This should match the admissions office name EXACTLY as it appears on the website. Examples (ONLY if these exact names appear on the website): 
   - "Graduate Admissions"
   - "Undergraduate Admissions"
   - "Graduate School Admissions"
   - "Office of Graduate Admissions"
   - "Admissions Office"
   
   ⚠️ DO NOT confuse this with "School":
   - ❌ "School of Engineering" - this goes in "School" field, NOT Department Placement
   - ❌ "College of Business" - this goes in "School" field, NOT Department Placement
   - ✅ "Graduate Admissions" - this goes in Department Placement
   - ✅ "Undergraduate Admissions" - this goes in Department Placement
   
   Rules:
   - "Department" = Admissions office (for applications)
   - "School" = Organizational unit (like School of Engineering)
   - Use the EXACT, FULL admissions office name as it appears on the website - copy it word-for-word
   - Look in these places for admissions office names:
     * "How to Apply" sections on program pages
     * "Admissions" or "Application" pages
     * Contact information sections
     * "Apply Now" or "Application Process" pages
     * Footer links or navigation menus with "Admissions"
     * Program detail pages that mention where to send applications
   - DO NOT assume "Undergraduate Admissions" just because it's an undergraduate program
   - DO NOT assume "Graduate Admissions" just because it's a graduate program
   - DO NOT confuse with school names - admissions offices are different from schools
   - DO NOT create generic admissions office names - only use names that are explicitly written on the website
   - DO NOT guess based on program level - only use names that are explicitly written on the website
   - If the admissions office name is not clearly visible on the website, use null
   - Accuracy is more important than completeness - use null rather than making something up

5. "Minimum Test Scores":
   - "Minimum A C T Score": Minimum ACT score
   - "Minimum Duo Lingo Score": Minimum Duolingo score
   - "Minimum E L S Score": Minimum ELS score
   - "Minimum G M A T Score": Minimum GMAT score
   - "Minimum Gre Score": Minimum GRE score
   - "Minimum I E L T S Score": Minimum IELTS score
   - "Minimum M A T Score": Minimum MAT score
   - "Minimum M C A T Score": Minimum MCAT score
   - "Minimum P T E Score": Minimum PTE score
   - "Minimum S A T Score": Minimum SAT score
   - "Minimum T O E F L Score": Minimum TOEFL score
   - "Minimum L S A T Score": Minimum LSAT score

Return the data as a JSON array where each element is a program object with all the above sections.

⚠️⚠️⚠️ BEFORE YOU CREATE THE JSON, REMEMBER:
- You are scraping "{college_name}"
- The "School" field must contain school names that are SPECIFIC to "{college_name}"
- DO NOT use school names from other universities
- If you cannot find the school name on "{college_name}"'s website, use null
- The examples below are GENERIC - use the actual school names from "{college_name}"'s website

CRITICAL INSTRUCTIONS:
- You MUST include EVERY SINGLE PROGRAM found on the website - no exceptions
- DO NOT limit the number of programs - if the university has 50, 100, 200, or 500 programs, return ALL of them

⚠️⚠️⚠️ CRITICAL - ACCURACY REQUIREMENTS (NO FABRICATION ALLOWED) ⚠️⚠️⚠️:

IMPORTANT: DO NOT guess, assume, or fabricate any information. Only use data that is EXPLICITLY STATED on the website. This applies especially to School and Department fields.

FOR SCHOOL FIELD:
- For EACH program, identify and include the "School" field ONLY if you can find it EXPLICITLY STATED on the website
- DO NOT guess school names based on program type
- DO NOT fabricate or invent school names
- DO NOT assume a school name - only use what is clearly written on the website
- If you cannot find the school name on the website, use null
- It is BETTER to use null than to guess incorrectly
- Examples of generic school name patterns (but use the ACTUAL names from the current university's website): "School of Medicine", "College of Engineering", "School of Business", "College of Arts and Sciences"
- ⚠️ CRITICAL: Each university has unique school names - do NOT copy names from examples or other universities
- NOTE: "School" refers to the organizational unit within the university (like School of Medicine, College of Engineering), NOT the university name itself

FOR DEPARTMENT/ADMISSIONS OFFICE FIELD:
- For EACH program, identify and include the "College Department I D" field ONLY if you can find it EXPLICITLY STATED on the website
- DO NOT guess department names based on program level (e.g., don't assume "Graduate Admissions" just because it's a graduate program)
- DO NOT fabricate or invent department/admissions office names
- DO NOT assume a department name - only use what is clearly written on the website
- If you cannot find the department/admissions office name on the website, use null
- It is BETTER to use null than to guess incorrectly
- Examples of correct department names (ONLY if found on website): "Graduate Admissions", "Undergraduate Admissions", "School of Business Admissions", "College of Engineering Admissions Office"
- The department name should match EXACTLY as it appears on the website
- DO NOT default to "Graduate Admissions" for graduate programs or "Undergraduate Admissions" for undergraduate programs unless you see these exact names on the website
- DO NOT stop after finding a few programs - continue until you have exhausted all program pages
- Visit EACH school's page, EACH college's page, EACH department's page to get their programs
- Explore all schools, colleges, departments, and program listing pages systematically
- If there are program listing pages with pagination, explore ALL pages
- Each program should be a separate object in the array
- Don't assume any data. Only provide data if it is available on the site
- For boolean fields, use true/false
- For dates, use YYYY-MM-DD format
- If information is not available, use null for that field
- The JSON array should contain ALL programs - there is no limit to the number of programs
- If you find a program list with "View All Programs" or "See All Majors" links, follow those links
- Return ONLY valid JSON array, no additional text or markdown formatting

Remember: This is a program listing page. Your goal is to find EVERY program listed on this page, not just a sample. Program listing pages typically have 20-100+ programs, so your array should reflect that."""
    
    return prompt

def process_college(args):
    """Process a single college - this function runs in a worker process."""
    college_id, college_name, website_url, idx, total, api_key, program_urls_cache = args
    
    # Each process needs its own engine and model
    engine = get_db_engine()
    if not engine:
        return {
            'college_id': college_id,
            'college_name': college_name,
            'success': False,
            'error': 'Failed to create database engine',
            'programs_saved': 0,
            'programs_found': 0
        }
    
    # Initialize Gemini model for this process
    genai.configure(api_key=api_key)
    model = None
    model_candidates = ["gemini-3-pro-preview", "gemini-1.5-pro", "gemini-pro"]
    
    for candidate in model_candidates:
        try:
            model = genai.GenerativeModel(f"models/{candidate}")
            break
        except Exception:
            continue
    
    if model is None:
        return {
            'college_id': college_id,
            'college_name': college_name,
            'success': False,
            'error': 'Failed to initialize Gemini model',
            'programs_saved': 0,
            'programs_found': 0
        }
    
    result = {
        'college_id': college_id,
        'college_name': college_name,
        'success': True,
        'programs_saved': 0,
        'programs_found': 0,
        'error': None
    }
    
    try:
        print(f"[Process {os.getpid()}] [{idx}/{total}] Processing: {college_name}")
        
        if not website_url:
            print(f"[Process {os.getpid()}] [{idx}/{total}] ⚠️  Skipping {college_name}: No website URL found")
            result['success'] = False
            result['error'] = 'No website URL'
            return result
        
        # Check if this college already has programs
        if check_college_has_programs(engine, college_id):
            print(f"[Process {os.getpid()}] [{idx}/{total}] ⏭️  Skipping {college_name}: Already has programs")
            result['success'] = True
            result['skipped'] = True
            return result
        
        # Find matching cache entry
        matched_cache_name, grad_programs_url, undergrad_programs_url = find_matching_cache_entry(
            college_name, program_urls_cache
        )
        
        # Build URLs to scrape
        urls_to_scrape = []
        if grad_programs_url:
            urls_to_scrape.append(("Graduate", grad_programs_url.strip()))
        if undergrad_programs_url:
            urls_to_scrape.append(("Undergraduate", undergrad_programs_url.strip()))
        
        if not urls_to_scrape:
            urls_to_scrape.append(("All Programs", website_url))
        
        all_programs = []
        
        # Scrape from each URL
        for url_type, url in urls_to_scrape:
            try:
                prompt = get_prompt(college_name, url_type)
                response = model.generate_content([url, prompt])
                
                if response and response.text:
                    programs = parse_json_response(response.text)
                    if programs and isinstance(programs, list):
                        print(f"[Process {os.getpid()}]    ✓ Found {len(programs)} {url_type.lower()} programs")
                        all_programs.extend(programs)
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                print(f"[Process {os.getpid()}]    ✗ Error scraping {url_type}: {str(e)}")
                time.sleep(2)
        
        # Save all programs
        if all_programs:
            result['programs_found'] = len(all_programs)
            programs_saved = 0
            
            for program_data in all_programs:
                if save_program(engine, college_id, program_data):
                    programs_saved += 1
                else:
                    print(f"[Process {os.getpid()}]      ✗ Failed to save program")
            
            result['programs_saved'] = programs_saved
            print(f"[Process {os.getpid()}]  Summary: {programs_saved}/{len(all_programs)} programs saved for {college_name}")
        else:
            print(f"[Process {os.getpid()}]  ⚠️  No programs found for {college_name}")
            result['error'] = 'No programs found'
        
        engine.dispose()
        return result
        
    except Exception as e:
        print(f"[Process {os.getpid()}]  ✗ Error processing {college_name}: {str(e)}")
        result['success'] = False
        result['error'] = str(e)
        if engine:
            engine.dispose()
        return result

def main():
    """Main function to orchestrate multiprocessing."""
    global DB_CONFIG, API_KEY, PROGRAM_URLS_CACHE, MODEL_NAME
    
    # Connect to database
    print("="*80)
    print("STEP 1: CONNECTING TO DATABASE")
    print("="*80)
    engine = get_db_engine()
    if not engine:
        print("⚠️  Failed to connect to database. Exiting.")
        exit(1)
    
    print("✓ Connected to database successfully")
    
    # Get all colleges from database
    print("\n" + "="*80)
    print("STEP 2: LOADING COLLEGES FROM DATABASE")
    print("="*80)
    
    colleges = get_all_colleges(engine)
    if not colleges:
        print("No colleges found in database. Exiting.")
        exit(1)
    
    print(f"✓ Found {len(colleges)} colleges in database")
    engine.dispose()
    
    # Load program URLs cache
    PROGRAM_URLS_CACHE_FILE = 'university_program_urls_cache.json'
    
    if os.path.exists(PROGRAM_URLS_CACHE_FILE):
        try:
            with open(PROGRAM_URLS_CACHE_FILE, 'r', encoding='utf-8') as f:
                PROGRAM_URLS_CACHE = json.load(f)
            print(f"✓ Loaded program URLs for {len(PROGRAM_URLS_CACHE)} universities from cache")
        except Exception as e:
            print(f"⚠️  Warning: Could not load program URLs cache: {e}")
            PROGRAM_URLS_CACHE = {}
    else:
        PROGRAM_URLS_CACHE = {}
        print(f"⚠️  No program URLs cache found!")
    
    # Get API key
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        print("⚠️  Warning: GOOGLE_API_KEY not found in environment variables.")
        print("Exiting.")
        exit(1)
    
    # Filter colleges that need processing
    print("\n" + "="*80)
    print("STEP 3: FILTERING COLLEGES")
    print("="*80)
    
    # Quick check which colleges need processing (using a single connection)
    temp_engine = get_db_engine()
    colleges_to_process = []
    for idx, (college_id, college_name, website_url) in enumerate(colleges, 1):
        if not website_url:
            continue
        if not check_college_has_programs(temp_engine, college_id):
            colleges_to_process.append((college_id, college_name, website_url, len(colleges_to_process) + 1, 0, API_KEY, PROGRAM_URLS_CACHE))
    temp_engine.dispose()
    
    # Update total count in each tuple
    total_count = len(colleges_to_process)
    colleges_to_process = [(c[0], c[1], c[2], c[3], total_count, c[5], c[6]) for c in colleges_to_process]
    
    print(f"✓ {len(colleges_to_process)} colleges need processing")
    
    if not colleges_to_process:
        print("No colleges to process. Exiting.")
        return
    
    # Determine number of processes
    num_processes = int(os.getenv("NUM_PROCESSES", cpu_count()))
    print(f"\n" + "="*80)
    print(f"STEP 4: STARTING MULTIPROCESSING WITH {num_processes} PROCESSES")
    print("="*80)
    
    # Process colleges in parallel
    start_time = time.time()
    
    with Pool(processes=num_processes) as pool:
        results = pool.map(process_college, colleges_to_process)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Aggregate results
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    
    total_programs_saved = 0
    total_programs_found = 0
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    for result in results:
        if result.get('skipped'):
            skipped_count += 1
        elif result.get('success'):
            success_count += 1
            total_programs_saved += result.get('programs_saved', 0)
            total_programs_found += result.get('programs_found', 0)
        else:
            error_count += 1
    
    print(f"Total colleges processed: {len(colleges_to_process)}")
    print(f"Colleges skipped (already have programs): {skipped_count}")
    print(f"Colleges successfully processed: {success_count}")
    print(f"Colleges with errors: {error_count}")
    print(f"Total programs found: {total_programs_found}")
    print(f"Total programs saved: {total_programs_saved}")
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    if len(colleges_to_process) > 0:
        print(f"Average time per college: {elapsed_time / len(colleges_to_process):.2f} seconds")
    
    print("\n" + "="*80)
    print("COMPLETED")
    print("="*80)

if __name__ == "__main__":
    main()

