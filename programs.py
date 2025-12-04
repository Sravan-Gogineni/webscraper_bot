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

# Load environment variables
load_dotenv()

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
    
    print(f"Attempting to connect to: {server}")
    print(f"Database: {database}")
    print(f"Username: {username}")
    print(f"Connection timeout: {connection_timeout}s, Login timeout: {login_timeout}s")
    
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
        # Test connection
        print("Testing connection...")
        with engine.connect() as conn:
            conn.execute(select(1))
        print("✓ Connection test successful")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print(f"\nTroubleshooting tips:")
        print(f"1. Verify the server address '{server}' is correct and reachable")
        print(f"2. Check if the SQL Server is running and accepting connections")
        print(f"3. Verify firewall rules allow connections on port 1433 (or your configured port)")
        print(f"4. Check if the database '{database}' exists")
        print(f"5. Verify username '{username}' and password are correct")
        print(f"6. Try increasing timeout values by setting DB_CONNECTION_TIMEOUT and DB_LOGIN_TIMEOUT environment variables")
        import traceback
        traceback.print_exc()
        return None

def get_all_colleges(engine):
    """Get all colleges from the database."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
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
        metadata.reflect(bind=engine)
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
        print(f"⚠️  Error checking programs for college ID {college_id}: {e}")
        return False

def find_college_department(engine, college_id, department_name, program_level=None):
    """Find CollegeDepartmentID by college and department name with multiple matching strategies."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        
        if college_department_table is None or department_table is None:
            return None
        
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
            
            # NO FALLBACK STRATEGIES - only return matches if department name was explicitly provided
            # We do NOT guess departments based on program level or pick random departments
            # Return None if no match found - it's better to skip the link than to guess incorrectly
            return None
            
    except Exception as e:
        print(f"Error finding college department: {e}")
        import traceback
        traceback.print_exc()
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
        print(f"Warning: Could not parse JSON. Error: {e}")
        print(f"Raw text (first 500 chars): {text[:500]}")
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
        metadata.reflect(bind=engine)
        
        program_table = metadata.tables.get("Program")
        program_req_table = metadata.tables.get("ProgramRequirements")
        program_term_table = metadata.tables.get("ProgramTermDetails")
        program_test_table = metadata.tables.get("ProgramTestScores")
        program_link_table = metadata.tables.get("ProgramDepartmentLink")
        
        if program_table is None:
            print("Error: Program table not found.")
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
            
            # Save ProgramDepartmentLink (only if department name is explicitly provided)
            if program_link_table is not None:
                dept_name = None
                if dept_placement:
                    dept_name = dept_placement.get("College Department I D") or dept_placement.get("College Department ID") or dept_placement.get("Department Name")
                
                # Only try to find department if name is explicitly provided (no guessing based on program level)
                college_dept_id = None
                if dept_name:
                    print(f"      Trying to find department: {dept_name}")
                    college_dept_id = find_college_department(engine, college_id, dept_name, level)
                    if college_dept_id:
                        print(f"      ✓ Found department match: {dept_name}")
                    else:
                        print(f"      ⚠️  Could not find matching department: {dept_name}")
                        print(f"      Program will be saved but not linked to a department")
                
                # NO FALLBACK - only link if department name was explicitly provided and found
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
                        print(f"      ✓ Updated ProgramDepartmentLink")
                    else:
                        conn.execute(
                            program_link_table.insert().values(
                                CollegeID=college_id,
                                ProgramID=program_id,
                                CollegeDepartmentID=college_dept_id
                            )
                        )
                        print(f"      ✓ Created ProgramDepartmentLink")
                else:
                    if not dept_name:
                        print(f"      ℹ️  No department name provided for program - skipping department link")
                    else:
                        print(f"      ⚠️  Department name provided but not found in database - skipping department link")
                    print(f"      Program will be saved but not linked to a department")
        
        return True
        
    except Exception as e:
        print(f"Error saving program: {e}")
        import traceback
        traceback.print_exc()
        return False

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

# Load program URLs cache
PROGRAM_URLS_CACHE_FILE = 'university_program_urls_cache.json'

def load_program_urls_cache():
    """Load program URLs from cache file."""
    if os.path.exists(PROGRAM_URLS_CACHE_FILE):
        try:
            with open(PROGRAM_URLS_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not load program URLs cache: {e}")
            return {}
    return {}

program_urls_cache = load_program_urls_cache()
if program_urls_cache:
    print(f"✓ Loaded program URLs for {len(program_urls_cache)} universities from cache")
    # Count how many have valid URLs
    valid_count = 0
    for cached_data in program_urls_cache.values():
        grad_url = cached_data.get("Graduate Programs URL")
        undergrad_url = cached_data.get("Undergraduate Programs URL")
        if (grad_url and grad_url.lower() != 'null' and grad_url.strip()) or \
           (undergrad_url and undergrad_url.lower() != 'null' and undergrad_url.strip()):
            valid_count += 1
    print(f"✓ Found valid program URLs for {valid_count} universities")
    print(f"ℹ️  Will prioritize dedicated program URLs over main website URLs")
else:
    print(f"⚠️  No program URLs cache found!")
    print(f"⚠️  Will use main website URLs (may miss programs)")
    print(f"⚠️  Consider running prog.py first to populate program URLs cache")

# Initiate Gemini API
print("\n" + "="*80)
print("STEP 3: INITIALIZING GEMINI API")
print("="*80)

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("⚠️  Warning: GOOGLE_API_KEY not found in environment variables.")
    print("Exiting.")
    exit(1)

genai.configure(api_key=api_key)

# Try to get the model
model = None
model_candidates = ["gemini-3-pro-preview", "gemini-1.5-pro", "gemini-pro"]

for candidate in model_candidates:
    try:
        model = genai.GenerativeModel(f"models/{candidate}")
        print(f"✓ Using model: {candidate}")
        break
    except Exception:
        continue

if model is None:
    print("⚠️  Error: Could not initialize any Gemini model. Exiting.")
    exit(1)

# Process each college
print("\n" + "="*80)
print("STEP 4: SCRAPING PROGRAMS DATA")
print("="*80)

success_count = 0
error_count = 0
total_programs = 0

for idx, (college_id, college_name, website_url) in enumerate(colleges, 1):
    if not website_url:
        print(f"\n[{idx}/{len(colleges)}] ⚠️  Skipping {college_name}: No website URL found")
        continue
    
    # Check if this college already has programs in the database
    if check_college_has_programs(engine, college_id):
        print(f"\n[{idx}/{len(colleges)}] ⏭️  Skipping {college_name}: Already has programs in database")
        continue
    
    print(f"\n[{idx}/{len(colleges)}] Processing: {college_name}")
    print(f"Main Website: {website_url}")
    
    try:
        # Get program URLs from cache
        grad_programs_url = None
        undergrad_programs_url = None
        
        # Try to find matching university in cache (case-insensitive with better matching)
        college_name_lower = college_name.lower().strip()
        matched_cache_name = None
        
        # Common generic words to ignore (these appear in many university names)
        common_generic_words = {'university', 'college', 'school', 'institute', 'institution', 'academy', 'center', 'centre'}
        
        # First try exact match
        for cached_name, cached_data in program_urls_cache.items():
            cached_name_lower = cached_name.lower().strip()
            if college_name_lower == cached_name_lower:
                matched_cache_name = cached_name
                grad_programs_url = cached_data.get("Graduate Programs URL")
                undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
                break
        
        # If no exact match, try partial matches (removing common generic words)
        if not matched_cache_name:
            
            # Extract meaningful words (ignore generic words and short words)
            college_words = set([w for w in college_name_lower.split() 
                                if len(w) > 3 and w not in common_generic_words])
            
            best_match = None
            best_score = 0
            
            for cached_name, cached_data in program_urls_cache.items():
                cached_name_lower = cached_name.lower().strip()
                cached_words = set([w for w in cached_name_lower.split() 
                                   if len(w) > 3 and w not in common_generic_words])
                
                # Calculate match score using only meaningful words
                if college_words and cached_words:
                    common_words = college_words & cached_words
                    if common_words:  # Must have at least one meaningful word in common
                        score = len(common_words) / max(len(college_words), len(cached_words))
                        
                        # Require at least 60% overlap AND at least one unique word match
                        if score > best_score and score >= 0.6:
                            best_score = score
                            best_match = (cached_name, cached_data)
            
            if best_match:
                matched_cache_name, cached_data = best_match
                grad_programs_url = cached_data.get("Graduate Programs URL")
                undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
        
        # If still no match, try fuzzy string matching (only for very similar names)
        if not matched_cache_name:
            for cached_name, cached_data in program_urls_cache.items():
                cached_name_lower = cached_name.lower().strip()
                
                # Only match if one name contains the other as a significant substring
                # AND they share at least 15 characters (excluding generic words)
                if college_name_lower in cached_name_lower or cached_name_lower in college_name_lower:
                    # Extract meaningful parts (remove generic words)
                    college_meaningful = ' '.join([w for w in college_name_lower.split() 
                                                   if w not in common_generic_words])
                    cached_meaningful = ' '.join([w for w in cached_name_lower.split() 
                                                  if w not in common_generic_words])
                    
                    # Check if meaningful parts overlap significantly (at least 10 chars)
                    if college_meaningful and cached_meaningful:
                        if college_meaningful in cached_meaningful or cached_meaningful in college_meaningful:
                            if len(college_meaningful) >= 8 and len(cached_meaningful) >= 8:
                                matched_cache_name = cached_name
                                grad_programs_url = cached_data.get("Graduate Programs URL")
                                undergrad_programs_url = cached_data.get("Undergraduate Programs URL")
                                break
        
        if matched_cache_name:
            # Warn if the matched name is different from the college name (fuzzy match)
            if matched_cache_name.lower().strip() != college_name.lower().strip():
                print(f"⚠️  WARNING: Fuzzy matched '{college_name}' to cache entry '{matched_cache_name}'")
                print(f"⚠️  Please verify this is correct before proceeding!")
            else:
                print(f"✓ Exact match found in cache: '{matched_cache_name}'")
            
            print(f"✓ Using cache entry: '{matched_cache_name}'")
            # Filter out None/null URLs
            if grad_programs_url and grad_programs_url.lower() != 'null' and grad_programs_url.strip():
                print(f"  ✓ Graduate Programs URL: {grad_programs_url}")
            else:
                print(f"  ⚠️  No valid Graduate Programs URL in cache (value: {grad_programs_url})")
                grad_programs_url = None  # Ensure it's None if invalid
            if undergrad_programs_url and undergrad_programs_url.lower() != 'null' and undergrad_programs_url.strip():
                print(f"  ✓ Undergraduate Programs URL: {undergrad_programs_url}")
            else:
                print(f"  ⚠️  No valid Undergraduate Programs URL in cache (value: {undergrad_programs_url})")
                undergrad_programs_url = None  # Ensure it's None if invalid
        else:
            print(f"  ⚠️  No matching entry found in program URLs cache for: '{college_name}'")
            if program_urls_cache:
                print(f"  Available cache entries (first 5): {list(program_urls_cache.keys())[:5]}")
            else:
                print(f"  Cache is empty - run prog.py first to populate program URLs")
        
        # URLs to scrape from - PRIORITIZE program URLs from cache
        urls_to_scrape = []
        
        # Only add URLs that are valid (not None, not 'null', not empty)
        if grad_programs_url and grad_programs_url.lower() != 'null' and grad_programs_url.strip():
            urls_to_scrape.append(("Graduate", grad_programs_url.strip()))
            print(f"  ✓ Added Graduate Programs URL to scrape list")
        if undergrad_programs_url and undergrad_programs_url.lower() != 'null' and undergrad_programs_url.strip():
            urls_to_scrape.append(("Undergraduate", undergrad_programs_url.strip()))
            print(f"  ✓ Added Undergraduate Programs URL to scrape list")
        
        # If no program URLs found in cache, use main website as fallback ONLY
        if not urls_to_scrape:
            print(f"\n  ⚠️  WARNING: No valid program URLs found in cache for this university!")
            print(f"  ⚠️  Falling back to main website URL (this may miss programs)")
            print(f"  ⚠️  Consider running prog.py first to populate program URLs cache")
            urls_to_scrape.append(("All Programs", website_url))
        else:
            print(f"\n  ✓ Successfully loaded {len(urls_to_scrape)} dedicated program URL(s) from cache")
            print(f"  ✓ Will scrape from program-specific pages instead of main website")
        
        all_programs = []
        
        # Scrape from each URL
        for url_type, url in urls_to_scrape:
            print(f"\n  Scraping {url_type} programs from: {url}")
            
            try:
                # Create prompt for scraping programs - tailored to URL type
                if url_type == "Graduate":
                    program_focus = "GRADUATE programs (Master's, Doctorate/PhD, Graduate Certificates)"
                    level_hint = "These should be Master's, Doctorate, or Graduate Certificate level programs"
                elif url_type == "Undergraduate":
                    program_focus = "UNDERGRADUATE programs (Bachelor's degrees, Undergraduate Certificates)"
                    level_hint = "These should be Bachelor's or Undergraduate Certificate level programs"
                else:
                    program_focus = "ALL programs (both Undergraduate and Graduate)"
                    level_hint = "Include programs of all levels"
                
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

Example structure:
[
  {{
    "Program Snapshot": {{
      "Program Name": "Master of Science in Computer Science",
      "Level": "Master",
      "Concentration": null,
      "Description": "...",
      "Program Website URL": "...",
      "Accreditation": "...",
      "Qs World Ranking": "...",
      "School": "College of Engineering"
    }},
    {{
      "Program Snapshot": {{
        "Program Name": "Master of Business Administration",
        "Level": "Master",
        "Concentration": null,
        "Description": "...",
        "Program Website URL": "...",
        "Accreditation": "...",
        "Qs World Ranking": "...",
        "School": "School of Business"
    }},
    {{
      "Program Snapshot": {{
        "Program Name": "Doctor of Medicine",
        "Level": "Doctorate",
        "Concentration": null,
        "Description": "...",
        "Program Website URL": "...",
        "Accreditation": "...",
        "Qs World Ranking": "...",
        "School": "School of Medicine"
    }},
    {{
      "Program Snapshot": {{
        "Program Name": "Example Program Without Clear School",
        "Level": "Master",
        "Concentration": null,
        "Description": "...",
        "Program Website URL": "...",
        "Accreditation": "...",
        "Qs World Ranking": "...",
        "School": null
    }},
    "Application Checklist": {{
      "Resume": true,
      "Statement Of Purpose": true,
      ...
    }},
    "Term & Investment": {{
      "Term": "Fall",
      "Live Date": "2024-01-15",
      "Deadline Date": "2024-03-01",
      ...
    }},
    "Department Placement": {{
      "College Department I D": "Graduate Admissions"
    }},
    {{
      "Program Snapshot": {{
        "Program Name": "Example Program Without Clear Department",
        "Level": "Master",
        ...
        "School": null
    }},
    "Department Placement": {{
      "College Department I D": null
    }},
    "Minimum Test Scores": {{
      "Minimum I E L T S Score": 6.5,
      "Minimum T O E F L Score": 80,
      ...
    }}
  }},
  ...
]

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

                response = model.generate_content([url, prompt])
                
                if response and response.text:
                    # Parse JSON response
                    programs = parse_json_response(response.text)
                    
                    if programs and isinstance(programs, list):
                        print(f"    ✓ Found {len(programs)} {url_type.lower()} programs")
                        all_programs.extend(programs)
                    else:
                        print(f"    ⚠️  Could not parse {url_type.lower()} programs or data is not a list")
                else:
                    print(f"    ⚠️  No response received for {url_type.lower()} programs")
                
                # Add delay between URL scrapes
                time.sleep(2)
                
            except Exception as e:
                print(f"    ✗ Error scraping {url_type} programs: {str(e)}")
                time.sleep(2)
        
        # Process all collected programs (after scraping from all URLs)
        if all_programs:
            program_count = len(all_programs)
            print(f"\n  ✓ Total programs collected: {program_count}")
            
            # Warn if very few programs
            if program_count < 10:
                print(f"  ⚠️  WARNING: Only {program_count} programs found. Most universities have many more programs.")
            elif program_count < 30:
                print(f"  ⚠️  NOTE: {program_count} programs found. Some universities have more programs.")
            
            programs_saved = 0
            for program_idx, program_data in enumerate(all_programs, 1):
                program_name = program_data.get("Program Snapshot", {}).get("Program Name", f"Program {program_idx}")
                print(f"    [{program_idx}/{program_count}] Processing: {program_name}")
                
                if save_program(engine, college_id, program_data):
                    programs_saved += 1
                    total_programs += 1
                else:
                    print(f"      ✗ Failed to save program")
                    error_count += 1
            
            print(f"  Summary: {programs_saved}/{program_count} programs saved")
            success_count += programs_saved
        else:
            print(f"  ⚠️  No programs found from any URL")
            error_count += 1
        
        # Add delay to avoid rate limiting
        time.sleep(3)
        
    except Exception as e:
        print(f"  ✗ Error processing {college_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        error_count += 1
        time.sleep(3)

print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)
print(f"Total colleges processed: {len(colleges)}")
print(f"Total programs saved: {total_programs}")
print(f"Success count: {success_count}")
print(f"Errors: {error_count}")

print("\n" + "="*80)
print("COMPLETED")
print("="*80)
