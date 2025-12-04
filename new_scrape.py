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

def clean_numeric_value(value):
    """Clean and convert numeric values, removing commas and converting to int."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        # Remove commas, spaces, and other non-numeric characters except minus sign
        cleaned = value.replace(',', '').replace(' ', '').strip()
        # Extract numbers only (handles cases like "56,000" or "$1,234")
        numbers = re.findall(r'-?\d+', cleaned)
        if numbers:
            try:
                return int(numbers[0])
            except (ValueError, IndexError):
                return None
    return None

def map_fields_to_tables(scraped_data, college_name, website_url):
    """Map scraped fields to database table structures."""
    
    # Map fields to tables based on schema - clean numeric values
    college_data = {
        "CollegeName": scraped_data.get("CollegeName") or college_name,
        "CollegeSetting": scraped_data.get("CollegeSetting"),
        "TypeofInstitution": scraped_data.get("TypeofInstitution"),
        "Student_Faculty": scraped_data.get("Student_Faculty"),
        "NumberOfCampuses": clean_numeric_value(scraped_data.get("NumberOfCampuses")),
        "TotalFacultyAvailable": clean_numeric_value(scraped_data.get("TotalFacultyAvailable")),
        "TotalProgramsAvailable": clean_numeric_value(scraped_data.get("TotalProgramsAvailable")),
        "TotalStudentsEnrolled": clean_numeric_value(scraped_data.get("TotalStudentsEnrolled")),
        "TotalGraduatePrograms": clean_numeric_value(scraped_data.get("TotalGraduatePrograms")),
        "TotalInternationalStudents": clean_numeric_value(scraped_data.get("TotalInternationalStudents")),
        "TotalStudents": clean_numeric_value(scraped_data.get("TotalStudents")),
        "TotalUndergradMajors": clean_numeric_value(scraped_data.get("TotalUndergradMajors")),
        "CountriesRepresented": scraped_data.get("CountriesRepresented"),
    }
    
    address_data = {
        "Street1": scraped_data.get("Street1"),
        "Street2": scraped_data.get("Street2"),
        "County": scraped_data.get("County"),
        "City": scraped_data.get("City"),
        "State": scraped_data.get("State"),
        "Country": scraped_data.get("Country"),
        "ZipCode": scraped_data.get("ZipCode"),
    }
    
    contact_data = {
        "LogoPath": scraped_data.get("LogoPath"),
        "Phone": scraped_data.get("Phone"),
        "Email": scraped_data.get("Email"),
        "SecondaryEmail": scraped_data.get("SecondaryEmail"),
        "WebsiteUrl": scraped_data.get("WebsiteUrl") or website_url,
        "AdmissionOfficeUrl": scraped_data.get("AdmissionOfficeUrl"),
        "VirtualTourUrl": scraped_data.get("VirtualTourUrl"),
        "FinancialAidUrl": scraped_data.get("FinancialAidUrl"),
    }
    
    app_req_data = {
        "ApplicationFees": scraped_data.get("ApplicationFees"),
        "TuitionFees": scraped_data.get("TuitionFees"),
        "TestPolicy": scraped_data.get("TestPolicy"),
        "CoursesAndGrades": scraped_data.get("CoursesAndGrades"),
        "Recommendations": scraped_data.get("Recommendations"),
        "PersonalEssay": scraped_data.get("PersonalEssay"),
        "WritingSample": scraped_data.get("WritingSample"),
        "AdditionalInformation": scraped_data.get("AdditionalInformation"),
        "AdditionalDeadlines": scraped_data.get("AdditionalDeadlines"),
    }
    
    stats_data = {
        "GradAvgTuition": clean_numeric_value(scraped_data.get("GradAvgTuition")),
        "GradInternationalStudents": clean_numeric_value(scraped_data.get("GradInternationalStudents")),
        "GradScholarshipHigh": clean_numeric_value(scraped_data.get("GradScholarshipHigh")),
        "GradScholarshipLow": clean_numeric_value(scraped_data.get("GradScholarshipLow")),
        "GradTotalStudents": clean_numeric_value(scraped_data.get("GradTotalStudents")),
        "UGAvgTuition": clean_numeric_value(scraped_data.get("UGAvgTuition")),
        "UGInternationalStudents": clean_numeric_value(scraped_data.get("UGInternationalStudents")),
        "UGScholarshipHigh": clean_numeric_value(scraped_data.get("UGScholarshipHigh")),
        "UGScholarshipLow": clean_numeric_value(scraped_data.get("UGScholarshipLow")),
        "UGTotalStudents": clean_numeric_value(scraped_data.get("UGTotalStudents")),
    }
    
    social_data = {
        "Facebook": scraped_data.get("Facebook"),
        "Instagram": scraped_data.get("Instagram"),
        "Twitter": scraped_data.get("Twitter"),
        "Youtube": scraped_data.get("Youtube"),
        "Tiktok": scraped_data.get("Tiktok"),
        "LinkedIn": scraped_data.get("LinkedIn"),
    }
    
    # Remove None values
    college_data = {k: v for k, v in college_data.items() if v is not None}
    address_data = {k: v for k, v in address_data.items() if v is not None}
    contact_data = {k: v for k, v in contact_data.items() if v is not None}
    app_req_data = {k: v for k, v in app_req_data.items() if v is not None}
    stats_data = {k: v for k, v in stats_data.items() if v is not None}
    social_data = {k: v for k, v in social_data.items() if v is not None}
    
    return {
        "College": college_data,
        "Address": address_data,
        "ContactInformation": contact_data,
        "ApplicationRequirements": app_req_data,
        "StudentStatistics": stats_data,
        "SocialMedia": social_data,
    }

def upsert_single_row(conn, table, key_column, key_value, payload):
    """Insert or update a single row in a table."""
    if table is None or not payload:
        return
    
    existing = (
        conn.execute(
            select(table.c[key_column.name]).where(key_column == key_value)
        ).first()
        is not None
    )
    
    payload_with_key = payload.copy()
    payload_with_key[key_column.name] = key_value
    
    if existing:
        conn.execute(table.update().where(key_column == key_value).values(**payload_with_key))
    else:
        conn.execute(table.insert().values(**payload_with_key))

def get_all_college_names(engine) -> set:
    """Get all college names from database (normalized to lowercase for comparison)."""
    if not engine:
        return set()
    
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_table = metadata.tables.get("College")
        
        if college_table is None:
            return set()
        
        with engine.connect() as conn:
            result = conn.execute(select(college_table.c.CollegeName))
            # Normalize to lowercase and strip for case-insensitive comparison
            college_names = {str(name).strip().lower() for name in result if name}
            return college_names
    except Exception as e:
        print(f"    ⚠️  Error fetching college names: {str(e)}")
        return set()

def insert_college_data(engine, table_payloads, social_payloads):
    """Insert or update college data in the database."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        college_table = metadata.tables.get("College")
        address_table = metadata.tables.get("Address")
        contact_table = metadata.tables.get("ContactInformation")
        app_req_table = metadata.tables.get("ApplicationRequirements")
        stats_table = metadata.tables.get("StudentStatistics")
        social_table = metadata.tables.get("SocialMedia")
        
        college_values = table_payloads.get("College", {})
        if not college_values:
            return None
        
        with engine.begin() as conn:
            # Check if college already exists by name (case-insensitive)
            college_name = college_values.get("CollegeName")
            existing_college = None
            if college_name:
                existing = conn.execute(
                    select(college_table.c.CollegeID)
                    .where(func.upper(college_table.c.CollegeName) == func.upper(college_name))
                ).first()
                if existing:
                    existing_college = existing.CollegeID
            
            if existing_college:
                # Update existing college
                college_id = existing_college
                conn.execute(
                    college_table.update()
                    .where(college_table.c.CollegeID == college_id)
                    .values(**college_values)
                )
                print(f"    ✓ Updated existing college (ID: {college_id})")
            else:
                # Insert new college
                result = conn.execute(college_table.insert().values(**college_values))
                college_id = int(result.inserted_primary_key[0])
                print(f"    ✓ Created new college (ID: {college_id})")
            
            # Insert/update related tables
            if address_table is not None:
                payload = table_payloads.get("Address", {})
                if payload:
                    upsert_single_row(conn, address_table, address_table.c.CollegeID, college_id, payload)
                    print(f"    ✓ Updated Address")
            
            if contact_table is not None:
                payload = table_payloads.get("ContactInformation", {})
                if payload:
                    upsert_single_row(conn, contact_table, contact_table.c.CollegeID, college_id, payload)
                    print(f"    ✓ Updated ContactInformation")
            
            if app_req_table is not None:
                payload = table_payloads.get("ApplicationRequirements", {})
                if payload:
                    upsert_single_row(conn, app_req_table, app_req_table.c.CollegeID, college_id, payload)
                    print(f"    ✓ Updated ApplicationRequirements")
            
            if stats_table is not None:
                payload = table_payloads.get("StudentStatistics", {})
                if payload:
                    upsert_single_row(conn, stats_table, stats_table.c.CollegeID, college_id, payload)
                    print(f"    ✓ Updated StudentStatistics")
            
            # Handle social media (multiple rows)
            if social_table is not None and social_payloads:
                try:
                    existing_rows = conn.execute(
                        select(social_table).where(social_table.c.CollegeID == college_id)
                    ).mappings().all()
                    existing_map = {row["PlatformName"].lower(): row for row in existing_rows}
                    
                    for platform, url in social_payloads.items():
                        if url:
                            platform_lower = platform.lower()
                            existing_row = existing_map.get(platform_lower)
                            
                            if existing_row:
                                conn.execute(
                                    social_table.update()
                                    .where(social_table.c.SocialID == existing_row["SocialID"])
                                    .values(URL=url)
                                )
                            else:
                                conn.execute(
                                    social_table.insert().values(
                                        CollegeID=college_id, PlatformName=platform, URL=url
                                    )
                                )
                    print(f"    ✓ Updated SocialMedia")
                except Exception as e:
                    print(f"    ⚠️  Error updating SocialMedia: {str(e)}")
            
            return college_id
            
    except Exception as e:
        print(f"    ✗ Database error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# Read the Excel file
df = pd.read_excel('Univs-3.xlsx')

# Get the first column (by position, regardless of column name)
# Process ALL universities
list_of_univs = df.iloc[:, 0].tolist()
original_univ_count = len([u for u in list_of_univs if u and str(u).strip() and str(u).strip().lower() != 'nan'])

# URL cache file
URL_CACHE_FILE = 'university_urls_cache-1.json'

def load_url_cache():
    """Load previously found URLs from cache file."""
    if os.path.exists(URL_CACHE_FILE):
        try:
            with open(URL_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not load URL cache: {e}")
            return {}
    return {}

def save_url_cache(cache):
    """Save URLs to cache file."""
    try:
        with open(URL_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️  Warning: Could not save URL cache: {e}")

# Connect to database FIRST to filter out existing universities
print("="*80)
print("STEP 1: CONNECTING TO DATABASE AND FILTERING EXISTING UNIVERSITIES")
print("="*80)
engine = get_db_engine()
existing_colleges_normalized = set()  # Normalized names for fast lookup

if not engine:
    print("⚠️  Failed to connect to database. Will process all universities.")
    engine = None
else:
    # Check which colleges already exist in database
    print("\nChecking which colleges already exist in database...")
    print("  Fetching all college names from database (one-time query)...")
    
    # Fetch all college names from database once (much faster than individual queries)
    db_college_names = get_all_college_names(engine)
    print(f"  ✓ Found {len(db_college_names)} colleges in database")
    
    # Store in engine for later use (avoid re-fetching)
    engine._db_college_names = db_college_names
    
    skipped_count = 0
    
    print("\n  Checking each college in the list...")
    for college_name in list_of_univs:
        if college_name and str(college_name).strip() and str(college_name).strip().lower() != 'nan':
            college_name_clean = str(college_name).strip()
            college_name_normalized = college_name_clean.lower().strip()
            
            # Check against in-memory set (very fast)
            # Also check for partial matches (e.g., "Columbia University" matches "Columbia University in the City of New York")
            found_match = False
            if college_name_normalized in db_college_names:
                found_match = True
            else:
                # Try partial matching - check if any DB name contains this name or vice versa
                # Extract key words (remove common words like "university", "college", "the", "of", "in", "city")
                common_words = {'university', 'college', 'the', 'of', 'in', 'city', 'new', 'york', 'state', 'institute', 'school'}
                excel_words = set(college_name_normalized.split()) - common_words
                
                for db_name in db_college_names:
                    # Exact substring match
                    if college_name_normalized in db_name or db_name in college_name_normalized:
                        found_match = True
                        break
                    
                    # Word-based matching - if most key words match
                    db_words = set(db_name.split()) - common_words
                    if excel_words and db_words:
                        common_key_words = excel_words.intersection(db_words)
                        if len(common_key_words) >= min(2, len(excel_words) * 0.6):  # At least 60% of key words match
                            found_match = True
                            break
            
            if found_match:
                # Store normalized version for comparison
                existing_colleges_normalized.add(college_name_normalized)
                skipped_count += 1
                print(f"  ⏭️  [{skipped_count}] Skipping {college_name_clean}: Already exists in database")
    
    if existing_colleges_normalized:
        print(f"\n✓ Total: {skipped_count} colleges already in database (will skip completely)")
    else:
        print("✓ No existing colleges found. Will process all colleges.")

# Filter out existing universities from the list
list_of_univs_filtered = []
for college_name in list_of_univs:
    if college_name and str(college_name).strip() and str(college_name).strip().lower() != 'nan':
        college_name_clean = str(college_name).strip()
        college_name_normalized = college_name_clean.lower().strip()
        if college_name_normalized not in existing_colleges_normalized:
            list_of_univs_filtered.append(college_name_clean)

print(f"\n✓ Filtered list: {len(list_of_univs_filtered)}/{len(list_of_univs)} universities to process (skipped {len(list_of_univs) - len(list_of_univs_filtered)} already in database)")

# Update list_of_univs to the filtered version
list_of_univs = list_of_univs_filtered

# Load existing URLs from cache
print("\n" + "="*80)
print("STEP 2: LOADING CACHED URLs")
print("="*80)

results = load_url_cache()
if results:
    print(f"✓ Loaded {len(results)} URLs from cache file: {URL_CACHE_FILE}")
else:
    print(f"ℹ️  No cache file found. Will search for all URLs.")

# Initialize results dictionary with all universities from filtered list
for college in list_of_univs:
    if college not in results:
        results[college] = None

# Find universities that don't have URLs yet (need to search)
universities_to_search = [college for college in list_of_univs if college not in results or not results[college] or results[college] is None]

print("\n" + "="*80)
print("STEP 3: SEARCHING FOR .EDU WEBSITES")
print("="*80)

if universities_to_search:
    print(f"Need to search for {len(universities_to_search)} universities (not in cache or missing URLs)...")
    
    # Search for each college's website
    with DDGS() as ddgs:
        for i, college in enumerate(universities_to_search, 1):
            try:
                print(f"[{i}/{len(universities_to_search)}] Searching for: {college}")
                
                # Search for the college website - only .edu domains
                query = f"{college} site:.edu"
                website_url = None
                
                # Get search results - only accept .edu domains
                for r in ddgs.text(query, max_results=20):
                    url = None
                    if 'href' in r:
                        url = r['href']
                    elif 'url' in r:
                        url = r['url']
                    
                    if not url:
                        continue
                    
                    url_lower = url.lower()
                    
                    # Only accept .edu domains - check if domain ends with .edu
                    try:
                        if url_lower.startswith('http://') or url_lower.startswith('https://'):
                            # Remove protocol
                            without_protocol = url_lower.split('://', 1)[1]
                            # Get domain part (before first /)
                            domain = without_protocol.split('/')[0]
                            # Check if domain ends with .edu
                            if domain.endswith('.edu'):
                                website_url = url
                                break
                    except (IndexError, AttributeError):
                        continue
                
                results[college] = website_url
                
                if website_url:
                    print(f"  ✓ Found: {website_url}")
                else:
                    print(f"  ✗ Not found")
                
                # Add a small delay to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                results[college] = None
                time.sleep(1)
    
    # Save all URLs to cache (including newly found ones)
    print(f"\nSaving all {len(results)} URLs to cache...")
    save_url_cache(results)
    print(f"✓ Cache saved to {URL_CACHE_FILE}")
else:
    print("✓ All URLs already in cache. Using cached URLs (no searching needed).")

# Print summary - count URLs for universities in our filtered list
found_count = sum(1 for college in list_of_univs if college in results and results[college])
print(f"\nSummary: {found_count}/{len(list_of_univs)} universities have URLs available for scraping")

# Initiate Gemini API
print("\n" + "="*80)
print("STEP 4: SCRAPING DATA WITH GEMINI API")
print("="*80)

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("⚠️  Warning: GOOGLE_API_KEY not found in environment variables.")
    print("Skipping Gemini API scraping.")
    model = None
else:
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
        print("⚠️  Error: Could not initialize any Gemini model.")

if model:
    scraped_count = 0
    inserted_count = 0
    error_count = 0
    
    # Process each website URL (only universities that are not already in database)
    for college_name, website_url in results.items():
        # Skip universities not in our filtered list (already filtered out as existing)
        if college_name not in list_of_univs:
            continue
            
        if not website_url:
            print(f"\n⚠️  Skipping {college_name}: No website URL found")
            continue
        
        scraped_count += 1
        try:
            print(f"\n[{scraped_count}/{found_count}] Scraping: {college_name}")
            print(f"Website: {website_url}")
            
            prompt = """You are a higher education data scraper. You are given a website link of a university or college. You need to scrape the data from the website. I will provide the list of the fields that you need to scrape.

Below is the list of the fields that you need to scrape:

'CollegeName', 'LogoPath', 'Phone', 'Email', 'SecondaryEmail', 'Street1', 'Street2', 'County', 'City', 'State', 'Country', 'ZipCode', 'WebsiteUrl', 'AdmissionOfficeUrl', 'VirtualTourUrl', 'Facebook', 'Instagram', 'Twitter', 'Youtube', 'Tiktok', 'ApplicationFees', 'TestPolicy', 'CoursesAndGrades', 'Recommendations', 'PersonalEssay', 'WritingSample', 'FinancialAidUrl', 'AdditionalInformation', 'AdditionalDeadlines', 'TuitionFees', 'LinkedIn', 'NumberOfCampuses', 'TotalFacultyAvailable', 'TotalProgramsAvailable', 'TotalStudentsEnrolled', 'CollegeSetting', 'TypeofInstitution', 'CountriesRepresented', 'GradAvgTuition', 'GradInternationalStudents', 'GradScholarshipHigh', 'GradScholarshipLow', 'GradTotalStudents', 'Student_Faculty', 'TotalGraduatePrograms', 'TotalInternationalStudents', 'TotalStudents', 'TotalUndergradMajors', 'UGAvgTuition', 'UGInternationalStudents', 'UGScholarshipHigh', 'UGScholarshipLow', 'UGTotalStudents'

So, please scrape the above fields from this website: """ + website_url + """. Don't assume any data on your own. Do not fabricate any data. Only provide data if it is available on the site.

Return the data in a structured JSON format with the field names as keys and the scraped values as values. Only include fields that have actual data. Return ONLY valid JSON, no additional text or markdown formatting."""

            response = model.generate_content([website_url, prompt])
            
            if response and response.text:
                # Parse JSON response
                scraped_data = parse_json_response(response.text)
                
                if scraped_data:
                    print(f"  ✓ Successfully scraped data")
                    
                    # Map fields to database tables
                    table_payloads = map_fields_to_tables(scraped_data, college_name, website_url)
                    social_payloads = table_payloads.pop("SocialMedia", {})
                    
                    # Insert into database
                    if engine:
                        print(f"  Inserting into database...")
                        college_id = insert_college_data(engine, table_payloads, social_payloads)
                        if college_id:
                            inserted_count += 1
                        else:
                            error_count += 1
                    else:
                        print(f"  ⚠️  Database not connected. Skipping insert.")
                else:
                    print(f"  ⚠️  Could not parse scraped data")
                    error_count += 1
            else:
                print(f"  ⚠️  No response received")
                error_count += 1
            
            # Add delay to avoid rate limiting
            time.sleep(2)
            
        except Exception as e:
            print(f"  ✗ Error scraping {college_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            error_count += 1
            time.sleep(2)
    
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    skipped_from_db = original_univ_count - len(list_of_univs)
    print(f"Total universities in Excel: {original_univ_count}")
    if skipped_from_db > 0:
        print(f"Skipped (already in database): {skipped_from_db}")
    print(f"Universities processed: {len(list_of_univs)}")
    print(f"Websites found: {found_count}")
    print(f"Successfully scraped: {scraped_count}")
    if engine:
        print(f"Successfully inserted: {inserted_count}")
        print(f"Errors: {error_count}")

print("\n" + "="*80)
print("COMPLETED")
print("="*80)
