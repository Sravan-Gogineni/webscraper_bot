# import modules
import pandas as pd
from ddgs import DDGS
import time
import os
import json
import re
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
    
    if not all([database, username, password]):
        print("Error: Database credentials not set. Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD.")
        return None
    
    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_params)}"
    
    try:
        engine = create_engine(connection_url, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(select(1))
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
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

def find_or_create_department(engine, department_name):
    """Find or create a Department entry. Returns DepartmentID."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        department_table = metadata.tables.get("Department")
        
        if department_table is None:
            print("Error: Department table not found.")
            return None
        
        with engine.begin() as conn:
            # Try to find existing department - exact match first
            stmt = select(department_table).where(
                department_table.c.DepartmentName == department_name
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                return existing["DepartmentID"]
            
            # Try case-insensitive exact match
            stmt = select(department_table).where(
                func.upper(department_table.c.DepartmentName) == func.upper(department_name)
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                return existing["DepartmentID"]
            
            # Create new department
            result = conn.execute(
                department_table.insert().values(DepartmentName=department_name)
            )
            return result.inserted_primary_key[0]
    except Exception as e:
        print(f"Error finding/creating department: {e}")
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

def map_department_fields(office_data):
    """Map scraped fields to CollegeDepartment table structure."""
    
    if not office_data:
        return None
    
    dept_data = {
        "Email": office_data.get("Email"),
        "PhoneNumber": office_data.get("PhoneNumber"),
        "PhoneType": office_data.get("PhoneType"),
        "AdmissionUrl": office_data.get("AdmissionUrl"),
        "BuildingName": office_data.get("BuildingName"),
        "Street1": office_data.get("Street1"),
        "Street2": office_data.get("Street2"),
        "City": office_data.get("City"),
        "State": office_data.get("State"),
        "StateName": office_data.get("StateName"),
        "Country": office_data.get("Country"),
        "CountryCode": office_data.get("CountryCode"),
        "CountryName": office_data.get("CountryName"),
        "ZipCode": office_data.get("ZipCode"),
    }
    
    # Remove None values
    dept_data = {k: v for k, v in dept_data.items() if v is not None and v != ""}
    
    return dept_data if dept_data else None

def check_college_has_departments(engine, college_id):
    """Check if a college already has any departments in the database.
    Returns True if the college has at least one department, False otherwise."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_department_table = metadata.tables.get("CollegeDepartment")
        
        if college_department_table is None:
            return False
        
        with engine.connect() as conn:
            # Count how many departments this college has
            count_stmt = select(func.count(college_department_table.c.CollegeDepartmentID)).where(
                college_department_table.c.CollegeID == college_id
            )
            count = conn.execute(count_stmt).scalar() or 0
            
            return count > 0
            
    except Exception as e:
        print(f"Error checking existing departments: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_college_department(engine, college_id, department_id, dept_data):
    """Save or update CollegeDepartment record."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_department_table = metadata.tables.get("CollegeDepartment")
        
        if college_department_table is None:
            print("Error: CollegeDepartment table not found.")
            return False
        
        dept_data["CollegeID"] = college_id
        dept_data["DepartmentID"] = department_id
        
        with engine.begin() as conn:
            # Check if CollegeDepartment link already exists
            stmt = select(college_department_table).where(
                (college_department_table.c.CollegeID == college_id) &
                (college_department_table.c.DepartmentID == department_id)
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                # Update existing record
                conn.execute(
                    college_department_table.update()
                    .where(college_department_table.c.CollegeDepartmentID == existing["CollegeDepartmentID"])
                    .values(**dept_data)
                )
                return True
            else:
                # Create new record
                conn.execute(college_department_table.insert().values(**dept_data))
                return True
        
    except Exception as e:
        print(f"Error saving CollegeDepartment: {e}")
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
print("STEP 4: SCRAPING ADMISSIONS OFFICES DATA")
print("="*80)

success_count = 0
error_count = 0
skipped_count = 0

for idx, (college_id, college_name, website_url) in enumerate(colleges, 1):
    if not website_url:
        print(f"\n[{idx}/{len(colleges)}] ⚠️  Skipping {college_name}: No website URL found")
        continue
    
    # Check if college already has any departments
    has_departments = check_college_has_departments(engine, college_id)
    
    if has_departments:
        skipped_count += 1
        print(f"\n[{idx}/{len(colleges)}] ⏭️  Skipping {college_name}: Already has departments in database")
        continue
    
    print(f"\n[{idx}/{len(colleges)}] Processing: {college_name}")
    print(f"Website: {website_url}")
    
    try:
        # Create prompt for scraping admissions offices
        prompt = f"""You are a higher education data scraper. You are given a website link of a university or college. You need to scrape information for admission offices from the website.

REQUIREMENT: You MUST find and scrape BOTH of these offices:
1. Undergraduate Admissions
2. Graduate Admissions

Additionally, the university may have other admission offices such as:
- School of Health Sciences Admissions
- School of Business Admissions
- School of Engineering Admissions
- And any other school-specific or program-specific admission offices

Find ALL admission offices on the website, but you MUST include both "Undergraduate Admissions" and "Graduate Admissions" even if they share the same contact information. Each office may have different contact information, addresses, and locations. Do NOT assume that different offices share the same address or contact information - only use the same information if it is explicitly stated on the website.

For EACH admission office you find, scrape the following fields if available:

- Email: Email address of the admissions office
- PhoneNumber: Phone number of the admissions office
- PhoneType: Type of phone (e.g., "Main", "Toll-free", "Fax")
- AdmissionUrl: URL of the admissions office webpage
- BuildingName: Name of the building where the office is located
- Street1: Street address line 1
- Street2: Street address line 2 (if applicable)
- City: City name
- State: State abbreviation or name
- StateName: Full state name
- Country: Country name
- CountryCode: Country code (e.g., "US")
- CountryName: Full country name
- ZipCode: ZIP or postal code

Return the data in a structured JSON format where each key is the exact name of the admission office. You MUST include "Undergraduate Admissions" and "Graduate Admissions" as keys. The value for each key is an object with the fields above.

Example structure:
{{
    "Undergraduate Admissions": {{
        "Email": "...",
        "PhoneNumber": "...",
        "PhoneType": "...",
        "AdmissionUrl": "...",
        "BuildingName": "...",
        "Street1": "...",
        "Street2": "...",
        "City": "...",
        "State": "...",
        "StateName": "...",
        "Country": "...",
        "CountryCode": "...",
        "CountryName": "...",
        "ZipCode": "..."
    }},
    "Graduate Admissions": {{
        "Email": "...",
        "PhoneNumber": "...",
        "PhoneType": "...",
        "AdmissionUrl": "...",
        "BuildingName": "...",
        "Street1": "...",
        "Street2": "...",
        "City": "...",
        "State": "...",
        "StateName": "...",
        "Country": "...",
        "CountryCode": "...",
        "CountryName": "...",
        "ZipCode": "..."
    }},
    "School of Health Sciences Admissions": {{
        "Email": "...",
        "PhoneNumber": "...",
        ...
    }}
}}

Important:
- You MUST include both "Undergraduate Admissions" and "Graduate Admissions" in your response
- Use the EXACT office name as it appears on the website (e.g., "School of Health Sciences Admissions", not just "Health Sciences")
- Include ALL other admission offices you find in addition to Undergraduate and Graduate
- Each office should have its own entry with separate contact and address information
- Don't assume any data on your own. Do not fabricate any data. Only provide data if it is available on the site.
- If an office's information is not found, you can omit that office (except Undergraduate and Graduate Admissions - try your best to find them)

Return ONLY valid JSON, no additional text or markdown formatting."""

        response = model.generate_content([website_url, prompt])
        
        if response and response.text:
            # Parse JSON response
            scraped_data = parse_json_response(response.text)
            
            if scraped_data:
                print(f"  ✓ Successfully scraped data")
                
                # Check for required offices
                office_names = [name.lower() for name in scraped_data.keys()]
                has_undergrad = any("undergraduate" in name for name in office_names)
                has_graduate = any("graduate" in name for name in office_names)
                
                if not has_undergrad:
                    print(f"  ⚠️  WARNING: Undergraduate Admissions not found in scraped data")
                if not has_graduate:
                    print(f"  ⚠️  WARNING: Graduate Admissions not found in scraped data")
                
                # Process all admission offices found
                offices_saved = 0
                offices_skipped = 0
                required_offices_saved = 0
                
                # Process Undergraduate Admissions first (if found)
                undergrad_key = None
                for key in scraped_data.keys():
                    if "undergraduate" in key.lower():
                        undergrad_key = key
                        break
                
                if undergrad_key:
                    office_data = scraped_data[undergrad_key]
                    if isinstance(office_data, dict):
                        dept_data = map_department_fields(office_data)
                        if dept_data:
                            dept_id = find_or_create_department(engine, "Undergraduate Admissions")
                            if dept_id:
                                if save_college_department(engine, college_id, dept_id, dept_data):
                                    print(f"    ✓ Saved Undergraduate Admissions")
                                    offices_saved += 1
                                    required_offices_saved += 1
                                    success_count += 1
                                else:
                                    print(f"    ✗ Failed to save Undergraduate Admissions")
                                    error_count += 1
                            else:
                                print(f"    ✗ Failed to find/create Undergraduate Admissions department")
                                error_count += 1
                        else:
                            print(f"    ⚠️  No valid data found for Undergraduate Admissions")
                            offices_skipped += 1
                
                # Process Graduate Admissions second (if found)
                graduate_key = None
                for key in scraped_data.keys():
                    if "graduate" in key.lower() and "undergraduate" not in key.lower():
                        graduate_key = key
                        break
                
                if graduate_key:
                    office_data = scraped_data[graduate_key]
                    if isinstance(office_data, dict):
                        dept_data = map_department_fields(office_data)
                        if dept_data:
                            dept_id = find_or_create_department(engine, "Graduate Admissions")
                            if dept_id:
                                if save_college_department(engine, college_id, dept_id, dept_data):
                                    print(f"    ✓ Saved Graduate Admissions")
                                    offices_saved += 1
                                    required_offices_saved += 1
                                    success_count += 1
                                else:
                                    print(f"    ✗ Failed to save Graduate Admissions")
                                    error_count += 1
                            else:
                                print(f"    ✗ Failed to find/create Graduate Admissions department")
                                error_count += 1
                        else:
                            print(f"    ⚠️  No valid data found for Graduate Admissions")
                            offices_skipped += 1
                
                # Process all other admission offices found
                for office_name, office_data in scraped_data.items():
                    # Skip if already processed (Undergraduate or Graduate)
                    if office_name == undergrad_key or office_name == graduate_key:
                        continue
                    
                    if not isinstance(office_data, dict):
                        continue
                    
                    # Map fields for this office
                    dept_data = map_department_fields(office_data)
                    
                    if dept_data:
                        # Find or create department with the exact office name
                        dept_id = find_or_create_department(engine, office_name)
                        if dept_id:
                            if save_college_department(engine, college_id, dept_id, dept_data):
                                print(f"    ✓ Saved {office_name}")
                                offices_saved += 1
                                success_count += 1
                            else:
                                print(f"    ✗ Failed to save {office_name}")
                                error_count += 1
                        else:
                            print(f"    ✗ Failed to find/create department: {office_name}")
                            error_count += 1
                    else:
                        print(f"    ⚠️  No valid data found for {office_name}")
                        offices_skipped += 1
                
                if offices_saved == 0:
                    print(f"  ⚠️  No admission offices saved")
                else:
                    print(f"  Summary: {offices_saved} offices saved ({required_offices_saved}/2 required offices)")
                
            else:
                print(f"  ⚠️  Could not parse scraped data")
                error_count += 1
        else:
            print(f"  ⚠️  No response received")
            error_count += 1
        
        # Add delay to avoid rate limiting
        time.sleep(2)
        
    except Exception as e:
        print(f"  ✗ Error processing {college_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        error_count += 1
        time.sleep(2)

print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)
print(f"Total colleges in database: {len(colleges)}")
if skipped_count > 0:
    print(f"Skipped (already have departments): {skipped_count}")
print(f"Colleges processed: {len(colleges) - skipped_count}")
print(f"Successfully saved offices: {success_count}")
print(f"Errors: {error_count}")

print("\n" + "="*80)
print("COMPLETED")
print("="*80)
