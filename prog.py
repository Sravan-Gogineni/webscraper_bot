# import modules
from ddgs import DDGS
import time
import os
import json
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, select, func
from urllib.parse import quote_plus

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
        print("⚠️  Error: Database credentials not set.")
        print("⚠️  Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD in your environment.")
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
        print(f"⚠️  Error: Could not connect to database: {e}")
        return None

def get_all_colleges_from_db(engine):
    """Get all college names from the database College table."""
    if not engine:
        return []
    
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_table = metadata.tables.get("College")
        
        if college_table is None:
            print("⚠️  Error: College table not found in database.")
            return []
        
        with engine.connect() as conn:
            # Get all college names from the College table
            stmt = select(college_table.c.CollegeName).order_by(college_table.c.CollegeName)
            rows = conn.execute(stmt).fetchall()
            # Filter out None/null values and return as list of strings
            college_names = [str(row.CollegeName).strip() for row in rows 
                           if row.CollegeName and str(row.CollegeName).strip() 
                           and str(row.CollegeName).strip().lower() != 'nan']
            return college_names
    except Exception as e:
        print(f"⚠️  Error fetching colleges from database: {e}")
        import traceback
        traceback.print_exc()
        return []

def check_college_has_programs(engine, college_name):
    """Check if a college already has programs in the database.
    Returns True if the college has at least one program, False otherwise."""
    if not engine:
        return False
    
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_table = metadata.tables.get("College")
        program_link_table = metadata.tables.get("ProgramDepartmentLink")
        
        if college_table is None or program_link_table is None:
            return False
        
        with engine.connect() as conn:
            # Find college by name (case-insensitive)
            college_name_clean = str(college_name).strip()
            
            # Try to find college by exact name match (case-insensitive)
            college_stmt = select(college_table.c.CollegeID).where(
                func.upper(college_table.c.CollegeName) == func.upper(college_name_clean)
            )
            college_result = conn.execute(college_stmt).first()
            
            if not college_result:
                return False
            
            college_id = college_result.CollegeID
            
            # Count how many programs this college has
            count_stmt = select(func.count(program_link_table.c.LinkID)).where(
                program_link_table.c.CollegeID == college_id
            )
            count = conn.execute(count_stmt).scalar() or 0
            
            return count > 0
            
    except Exception as e:
        print(f"⚠️  Error checking programs for {college_name}: {e}")
        return False

# Connect to database and get college names
print("="*80)
print("STEP 0: CONNECTING TO DATABASE AND LOADING COLLEGE NAMES")
print("="*80)

engine = get_db_engine()
if not engine:
    print("❌ Cannot proceed without database connection. Exiting.")
    exit(1)

print("✓ Connected to database successfully")

# Get all college names from database
list_of_univs = get_all_colleges_from_db(engine)
original_univ_count = len(list_of_univs)

if not list_of_univs:
    print("⚠️  No colleges found in database. Exiting.")
    exit(1)

print(f"✓ Found {original_univ_count} colleges in database")

# Program URLs cache file
PROGRAM_URLS_CACHE_FILE = 'university_program_urls_cache.json'

def load_program_urls_cache():
    """Load previously found program URLs from cache file."""
    if os.path.exists(PROGRAM_URLS_CACHE_FILE):
        try:
            with open(PROGRAM_URLS_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not load program URLs cache: {e}")
            return {}
    return {}

def save_program_urls_cache(cache):
    """Save program URLs to cache file."""
    try:
        with open(PROGRAM_URLS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️  Warning: Could not save program URLs cache: {e}")

def search_program_url(college_name, program_type, ddgs):
    """Search for program URL (graduate or undergraduate) for a college."""
    # Search for the program URL - only .edu domains
    query = f'"{college_name}" {program_type} programs site:.edu'
    program_url = None
    
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
                    # Also check if URL contains program-related keywords
                    if any(keyword in url_lower for keyword in [program_type.lower(), 'program', 'major', 'degree']):
                        program_url = url
                        break
        except (IndexError, AttributeError):
            continue
    
    return program_url

# Filter out colleges that already have programs
print("\n" + "="*80)
print("STEP 1: FILTERING COLLEGES WITH PROGRAMS")
print("="*80)

colleges_with_programs = set()
skipped_count = 0

print("Checking which colleges already have programs in database...")

for idx, college_name in enumerate(list_of_univs, 1):
    if college_name and str(college_name).strip() and str(college_name).strip().lower() != 'nan':
        college_name_clean = str(college_name).strip()
        
        if check_college_has_programs(engine, college_name_clean):
            colleges_with_programs.add(college_name_clean)
            skipped_count += 1
            if skipped_count <= 10:  # Show first 10
                print(f"  ⏭️  [{skipped_count}] Skipping {college_name_clean}: Already has programs")

if skipped_count > 10:
    print(f"  ... and {skipped_count - 10} more colleges with programs")

if colleges_with_programs:
    print(f"\n✓ Total: {skipped_count} colleges already have programs (will skip)")
else:
    print("✓ No colleges with programs found. Will process all colleges.")

# Filter out colleges that already have programs
list_of_univs_filtered = []
for college_name in list_of_univs:
    if college_name and str(college_name).strip() and str(college_name).strip().lower() != 'nan':
        college_name_clean = str(college_name).strip()
        if college_name_clean not in colleges_with_programs:
            list_of_univs_filtered.append(college_name_clean)

print(f"\n✓ Filtered list: {len(list_of_univs_filtered)}/{original_univ_count} colleges to process (skipped {skipped_count} with programs)")

# Update list_of_univs to the filtered version
list_of_univs = list_of_univs_filtered

# Load existing program URLs from cache
print("\n" + "="*80)
print("STEP 2: LOADING CACHED PROGRAM URLs")
print("="*80)

results = load_program_urls_cache()
if results:
    print(f"✓ Loaded program URLs for {len(results)} universities from cache file: {PROGRAM_URLS_CACHE_FILE}")
else:
    print(f"ℹ️  No cache file found. Will search for all program URLs.")

# Initialize results dictionary with all universities from filtered list
for college in list_of_univs:
    if college not in results:
        results[college] = {
            "Graduate Programs URL": None,
            "Undergraduate Programs URL": None
        }

# Find universities that need program URLs searched
universities_to_search = []
for college in list_of_univs:
    college_data = results.get(college, {})
    if not college_data.get("Graduate Programs URL") or not college_data.get("Undergraduate Programs URL"):
        universities_to_search.append(college)

print("\n" + "="*80)
print("STEP 3: SEARCHING FOR PROGRAM URLs")
print("="*80)

if universities_to_search:
    print(f"Need to search for program URLs for {len(universities_to_search)} universities...")
    
    # Search for each college's program URLs
    with DDGS() as ddgs:
        for i, college in enumerate(universities_to_search, 1):
            try:
                print(f"\n[{i}/{len(universities_to_search)}] Processing: {college}")
                
                # Search for Graduate Programs URL
                if not results[college].get("Graduate Programs URL"):
                    print(f"  Searching for Graduate Programs URL...")
                    grad_url = search_program_url(college, "graduate", ddgs)
                    if grad_url:
                        results[college]["Graduate Programs URL"] = grad_url
                        print(f"    ✓ Found: {grad_url}")
                    else:
                        print(f"    ✗ Not found")
                    time.sleep(1)
                
                # Search for Undergraduate Programs URL
                if not results[college].get("Undergraduate Programs URL"):
                    print(f"  Searching for Undergraduate Programs URL...")
                    undergrad_url = search_program_url(college, "undergraduate", ddgs)
                    if undergrad_url:
                        results[college]["Undergraduate Programs URL"] = undergrad_url
                        print(f"    ✓ Found: {undergrad_url}")
                    else:
                        print(f"    ✗ Not found")
                    time.sleep(1)
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                time.sleep(1)
    
    # Save all program URLs to cache
    print(f"\nSaving program URLs to cache...")
    save_program_urls_cache(results)
    print(f"✓ Cache saved to {PROGRAM_URLS_CACHE_FILE}")
else:
    print("✓ All program URLs already in cache. Using cached URLs (no searching needed).")

# Print summary
grad_found = sum(1 for v in results.values() if v.get("Graduate Programs URL"))
undergrad_found = sum(1 for v in results.values() if v.get("Undergraduate Programs URL"))
both_found = sum(1 for v in results.values() if v.get("Graduate Programs URL") and v.get("Undergraduate Programs URL"))

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total universities in database: {original_univ_count}")
if skipped_count > 0:
    print(f"Skipped (already have programs): {skipped_count}")
print(f"Universities processed: {len(list_of_univs)}")
print(f"Graduate Programs URLs found: {grad_found}/{len(list_of_univs)}")
print(f"Undergraduate Programs URLs found: {undergrad_found}/{len(list_of_univs)}")
print(f"Both URLs found: {both_found}/{len(list_of_univs)}")

print("\n" + "="*80)
print("COMPLETED")
print("="*80)
